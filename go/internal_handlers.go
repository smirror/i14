package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"math"
	"net/http"
	"time"
)

// Chair represents a chair entity from the database
type Chair struct {
	ID        string
	Latitude  float64
	Longitude float64
}

// ChairWithDistance represents a chair with precomputed distance
type ChairWithDistance struct {
	ID        string
	Latitude  float64
	Longitude float64
	Distance  float64
}

// calculateDistance calculates the Haversine distance between two points
func calculateDistance(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadius = 6371.0 // Radius of the Earth in kilometers
	lat1Rad := lat1 * math.Pi / 180
	lng1Rad := lng1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180
	lng2Rad := lng2 * math.Pi / 180

	dLat := lat2Rad - lat1Rad
	dLng := lng2Rad - lng1Rad

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dLng/2)*math.Sin(dLng/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadius * c
}

// findNearestChair searches for the nearest available chair in the bounding box
func findNearestChair(ctx context.Context, tx *sql.Tx, ride *Ride, minLat, maxLat, minLng, maxLng float64) (*ChairWithDistance, error) {
	// 椅子を検索
	rows, err := tx.QueryContext(ctx, `
		SELECT chairs.id, chair_locations.latitude, chair_locations.longitude
		FROM chairs
		INNER JOIN chair_locations ON chairs.id = chair_locations.chair_id
		WHERE chairs.is_active = TRUE
		  AND chairs.is_in_use = FALSE
		  AND chair_locations.latitude BETWEEN ? AND ?
		  AND chair_locations.longitude BETWEEN ? AND ?;
	`, minLat, maxLat, minLng, maxLng)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Goで距離を計算
	var nearestChair *ChairWithDistance
	for rows.Next() {
		chair := Chair{}
		if err := rows.Scan(&chair.ID, &chair.Latitude, &chair.Longitude); err != nil {
			return nil, err
		}

		// 距離を計算
		distance := calculateDistance(ride.PickupLatitude, ride.PickupLongitude, chair.Latitude, chair.Longitude)

		// 最小距離の椅子を更新
		if nearestChair == nil || distance < nearestChair.Distance {
			nearestChair = &ChairWithDistance{
				ID:        chair.ID,
				Latitude:  chair.Latitude,
				Longitude: chair.Longitude,
				Distance:  distance,
			}
		}
	}

	if nearestChair == nil {
		return nil, sql.ErrNoRows
	}

	return nearestChair, nil
}

// graduallyExpandSearch expands the search range to find the nearest available chair
func graduallyExpandSearch(ctx context.Context, tx *sql.Tx, ride *Ride, maxDistance, stepDistance float64) (*ChairWithDistance, error) {
	currentDistance := stepDistance
	for currentDistance <= maxDistance {
		// バウンディングボックスを計算
		minLat, maxLat, minLng, maxLng := calculateBoundingBox(ride.PickupLatitude, ride.PickupLongitude, currentDistance)

		// Go内で計算しながら最も近い椅子を検索
		chair, err := findNearestChair(ctx, tx, ride, minLat, maxLat, minLng, maxLng)
		if err == nil {
			return chair, nil // 椅子が見つかった場合
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err // データベースエラー
		}

		// 距離を拡大
		log.Printf("Expanding search range to %.2f km\n", currentDistance)
		currentDistance += stepDistance
		time.Sleep(100 * time.Millisecond) // オプション: 負荷軽減のために少し待つ
	}
	return nil, nil // 見つからなかった場合
}

// internalGetMatching matches rides with available chairs
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// トランザクションの開始
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	txl, err := db.BeginTx(ctx, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer txl.Rollback()

	// 未マッチのライドを取得
	ride := &Ride{}
	err = tx.QueryRowContext(ctx, `
		SELECT id, pickup_latitude, pickup_longitude
		FROM rides
		WHERE chair_id IS NULL
		ORDER BY created_at
		LIMIT 1 FOR UPDATE
	`).Scan(&ride.ID, &ride.PickupLatitude, &ride.PickupLongitude)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent) // 未マッチのライドなし
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 徐々に距離を広げて椅子を検索
	maxDistance := 150.0 // 最大距離 (km)
	stepDistance := 25.0 // 段階距離 (km)
	chair, err := graduallyExpandSearch(ctx, txl, ride, maxDistance, stepDistance)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if chair == nil {
		w.WriteHeader(http.StatusNoContent) // 空き椅子なし
		return
	}

	// ライドと椅子をマッチング
	_, err = tx.ExecContext(ctx, `
		UPDATE rides SET chair_id = ? WHERE id = ?
	`, chair.ID, ride.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 椅子の状態を更新
	_, err = tx.ExecContext(ctx, `
		UPDATE chairs SET is_in_use = TRUE WHERE id = ?
	`, chair.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// トランザクションコミット
	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
