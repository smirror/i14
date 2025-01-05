package main

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"time"
	"sync"
	"github.com/oklog/ulid/v2"
)

const (
	initialFare     = 500
	farePerDistance = 100
)

type ownerPostOwnersRequest struct {
	Name string `json:"name"`
}

type ownerPostOwnersResponse struct {
	ID                 string `json:"id"`
	ChairRegisterToken string `json:"chair_register_token"`
}

func ownerPostOwners(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &ownerPostOwnersRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, errors.New("some of required fields(name) are empty"))
		return
	}

	ownerID := ulid.Make().String()
	accessToken := secureRandomStr(32)
	chairRegisterToken := secureRandomStr(32)

	_, err := db.ExecContext(
		ctx,
		"INSERT INTO owners (id, name, access_token, chair_register_token) VALUES (?, ?, ?, ?)",
		ownerID, req.Name, accessToken, chairRegisterToken,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "owner_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &ownerPostOwnersResponse{
		ID:                 ownerID,
		ChairRegisterToken: chairRegisterToken,
	})
}

type chairSales struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Sales int    `json:"sales"`
}

type modelSales struct {
	Model string `json:"model"`
	Sales int    `json:"sales"`
}

type ownerGetSalesResponse struct {
	TotalSales int          `json:"total_sales"`
	Chairs     []chairSales `json:"chairs"`
	Models     []modelSales `json:"models"`
}

func ownerGetSales(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	since := time.Unix(0, 0)
	until := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	if r.URL.Query().Get("since") != "" {
		parsed, err := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		since = time.UnixMilli(parsed)
	}
	if r.URL.Query().Get("until") != "" {
		parsed, err := strconv.ParseInt(r.URL.Query().Get("until"), 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		until = time.UnixMilli(parsed)
	}

	owner := r.Context().Value("owner").(*Owner)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	chairs := []Chair{}
	if err := tx.SelectContext(ctx, &chairs, "SELECT * FROM chairs WHERE owner_id = ?", owner.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	res := ownerGetSalesResponse{
		TotalSales: 0,
	}

	modelSalesByModel := map[string]int{}
	for _, chair := range chairs {
		rides := []Ride{}
		if err := tx.SelectContext(ctx, &rides, "SELECT rides.* FROM rides JOIN ride_statuses ON rides.id = ride_statuses.ride_id WHERE chair_id = ? AND status = 'COMPLETED' AND updated_at BETWEEN ? AND ? + INTERVAL 999 MICROSECOND", chair.ID, since, until); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		sales := sumSales(rides)
		res.TotalSales += sales

		res.Chairs = append(res.Chairs, chairSales{
			ID:    chair.ID,
			Name:  chair.Name,
			Sales: sales,
		})

		modelSalesByModel[chair.Model] += sales
	}

	models := []modelSales{}
	for model, sales := range modelSalesByModel {
		models = append(models, modelSales{
			Model: model,
			Sales: sales,
		})
	}
	res.Models = models

	writeJSON(w, http.StatusOK, res)
}

func sumSales(rides []Ride) int {
	sale := 0
	for _, ride := range rides {
		sale += calculateSale(ride)
	}
	return sale
}

func calculateSale(ride Ride) int {
	return calculateFare(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
}

type chairWithDetail struct {
	ID                     string       `db:"id"`
	OwnerID                string       `db:"owner_id"`
	Name                   string       `db:"name"`
	AccessToken            string       `db:"access_token"`
	Model                  string       `db:"model"`
	IsActive               bool         `db:"is_active"`
	CreatedAt              time.Time    `db:"created_at"`
	UpdatedAt              time.Time    `db:"updated_at"`
	TotalDistance          int          `db:"total_distance"`
	TotalDistanceUpdatedAt sql.NullTime `db:"total_distance_updated_at"`
}

type ownerGetChairResponse struct {
	Chairs []ownerGetChairResponseChair `json:"chairs"`
}

type ownerGetChairResponseChair struct {
	ID                     string `json:"id"`
	Name                   string `json:"name"`
	Model                  string `json:"model"`
	Active                 bool   `json:"active"`
	RegisteredAt           int64  `json:"registered_at"`
	TotalDistance          int    `json:"total_distance"`
	TotalDistanceUpdatedAt *int64 `json:"total_distance_updated_at,omitempty"`
}

type CacheItem struct {
	Value     int
	ExpiresAt time.Time
}
var totalDistanceCache sync.Map

type chairID struct {
	ID                     string       `db:"id"`
}

func ownerGetChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	owner := ctx.Value("owner").(*Owner)

	// キャッシュから取得
	// まずはownerが所有する椅子すべてを取得し、キャッシュに存在するか確認
	// キャッシュに存在する場合は、キャッシュから取得し、存在しない場合はDBから取得
	chairIds := []chairID{}
	if err := db.SelectContext(ctx, &chairIds, `SELECT id FROM Id WHERE owner_id = ?`, owner.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// totalDistance をキャッシュから取得する
	totalDistanceMap := map[string]int{}
	for _, chair := range chairIds {
		if item, ok := totalDistanceCache.Load(chair.ID); ok {
			cached := item.(CacheItem)
			if time.Now().Before(cached.ExpiresAt) {
				totalDistanceMap[chair.ID] = cached.Value
			} else {
				// キャッシュが期限切れの場合はDBから取得
				var totalDistance sql.NullInt64
				if err := db.GetContext(ctx, &totalDistance, `
				SELECT
					IFNULL(
						SUM(ABS(latitude - LAG(latitude) OVER(PARTITION BY chair_id ORDER BY created_at)) + ABS(longitude - LAG(longitude) OVER(PARTITION BY chair_id ORDER BY created_at)), 0) AS total_distance
						FROM
							chair_locations
							WHERE
								chair_id = ?`, chair.ID); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
				if totalDistance.Valid {
					totalDistanceMap[chair.ID] = int(totalDistance.Int64)
				} else {
					totalDistanceMap[chair.ID] = 0
				}

				totalDistanceCache.Store(chair.ID, CacheItem{Value: totalDistanceMap[chair.ID]})
			}
		} else {
			// TODO: DRY にする
			// キャッシュが存在しない場合はDBから取得
			var totalDistance sql.NullInt64
			if err := db.GetContext(ctx, &totalDistance, `
			SELECT
				IFNULL(
					SUM(ABS(latitude - LAG(latitude) OVER(PARTITION BY chair_id ORDER BY created_at)) + ABS(longitude - LAG(longitude) OVER(PARTITION BY chair_id ORDER BY created_at)), 0) AS total_distance
					FROM
						chair_locations
						WHERE
							chair_id = ?`, chair.ID); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			if totalDistance.Valid {
				totalDistanceMap[chair.ID] = int(totalDistance.Int64)
			} else {
				totalDistanceMap[chair.ID] = 0
			}

			totalDistanceCache.Store(chair.ID, CacheItem{Value: totalDistanceMap[chair.ID]})
		}
	}

	chairs := []chairWithDetail{}
	if err := db.SelectContext(ctx, &chairs, `WITH aggregated_distances AS(
			SELECT
				chair_id,
				MAX(created_at) AS latest_distance_update
			FROM
				chair_locations
			GROUP BY
				chair_id
		)
		SELECT
			c.id,
			c.owner_id,
			c.name,
			c.access_token,
			c.model,
			c.is_active,
			c.created_at,
			c.updated_at,
			ad.latest_distance_update
		FROM
			chairs c
			LEFT JOIN
				aggregated_distances ad
			ON	c.id = ad.chair_id
		WHERE
			c.owner_id = ?
`, owner.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	res := ownerGetChairResponse{}
	for _, chair := range chairs {
		c := ownerGetChairResponseChair{
			ID:            chair.ID,
			Name:          chair.Name,
			Model:         chair.Model,
			Active:        chair.IsActive,
			RegisteredAt:  chair.CreatedAt.UnixMilli(),
			TotalDistance: totalDistanceMap[chair.ID],
		}
		if chair.TotalDistanceUpdatedAt.Valid {
			t := chair.TotalDistanceUpdatedAt.Time.UnixMilli()
			c.TotalDistanceUpdatedAt = &t
		}
		res.Chairs = append(res.Chairs, c)
	}
	writeJSON(w, http.StatusOK, res)
}
