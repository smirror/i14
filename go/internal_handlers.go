package main

import (
	"net/http"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	// 決まっていないライドと空いている椅子を全て取得
	rides := []Ride{}
	if err := db.Select(&rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at ASC`); err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(rides) <= 0 {
		return
	}

	chairs := []ChairWithLatLon{}
	if err := db.Select(&chairs, `
 WITH chair_latest_location AS (
 	SELECT *
 	FROM (
 		SELECT chair_locations.*, ROW_NUMBER() OVER (PARTITION BY chair_id ORDER BY created_at DESC) AS rn
 		FROM chair_locations
 	) c
 	WHERE c.rn = 1
 ),
 chair_latest_status AS (
 	SELECT *
 	FROM (
 		SELECT rides.*, ride_statuses.status AS ride_status, ROW_NUMBER() OVER (PARTITION BY chair_id ORDER BY ride_statuses.created_at DESC) AS rn
 		FROM rides INNER JOIN ride_statuses ON rides.id = ride_statuses.ride_id AND ride_statuses.chair_sent_at IS NOT NULL -- この条件は椅子の通知エンドポイントの実装で、未送信の状態がある2つ以上の異なるライドが割り当てられていても正しく順番に送るように修正していれば不要
 	) r
 	WHERE r.rn = 1
 )
 SELECT
 	chairs.*, chair_latest_location.latitude, chair_latest_location.longitude
 FROM chairs
 LEFT JOIN chair_latest_status ON chairs.id = chair_latest_status.chair_id
 LEFT JOIN chair_latest_location ON chairs.id = chair_latest_location.chair_id
 WHERE
 	(chair_latest_status.ride_status = 'COMPLETED' OR chair_latest_status.ride_status IS NULL) AND chairs.is_active`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	for _, ride := range rides {
		minDistance := 400
		var minChair *ChairWithLatLon
		var minChairIdx int
		for idx, chair := range chairs {
			distance := calculateDistance(chair.Latitude, chair.Longitude, ride.PickupLatitude, ride.PickupLongitude)
			if distance < minDistance {
				minDistance = distance
				minChair = &chair
				minChairIdx = idx
			}
		}
		if minChair != nil {
			// 複数のmatcherが動く場合には、複数の椅子が同じライドに割り当てられないようトランザクションなどで排他制御を行う必要がある
			if _, err := db.Exec("UPDATE rides SET chair_id = ? WHERE id = ?", minChair.ID, ride.ID); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}

			chairs = append(chairs[:minChairIdx], chairs[minChairIdx+1:]...)
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
