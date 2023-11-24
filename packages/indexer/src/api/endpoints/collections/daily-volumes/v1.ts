/* eslint-disable @typescript-eslint/no-explicit-any */

import { Request, RouteOptions } from "@hapi/hapi";
import Joi from "joi";

import { redb } from "@/common/db";
import { logger } from "@/common/logger";
import { formatEth } from "@/common/utils";

const version = "v1";

export const getDailyVolumesV1Options: RouteOptions = {
  description: "Daily collection volume",
  notes: "Get date, volume, rank and sales count for each collection",
  tags: ["api", "Stats"],
  plugins: {
    "hapi-swagger": {
      order: 7,
    },
  },
  validate: {
    query: Joi.object({
      id: Joi.string()
        .lowercase()
        .description(
          "Filter to a particular collection with collection-id. Example: `0x8d04a8c79ceb0889bdd12acdf3fa9d207ed3ff63`"
        )
        .required(),
      limit: Joi.number().default(60).description("Amount of items returned in response."),
      startTimestamp: Joi.number().description("The start timestamp you want to filter on (UTC)"),
      endTimestamp: Joi.number().description("The end timestamp you want to filter on (UTC)"),
    }),
  },
  response: {
    schema: Joi.object({
      collections: Joi.array().items(
        Joi.object({
          id: Joi.string(),
          timestamp: Joi.number(),
          volume: Joi.number().unsafe(true),
          rank: Joi.number(),
          floor_sell_value: Joi.number().unsafe(true).description("Native currency to chain."),
          sales_count: Joi.number(),
        }).allow(null)
      ),
    }).label(`getDailyVolumes${version.toUpperCase()}Response`),
    failAction: (_request, _h, error) => {
      logger.error(`get-daily-volumes-${version}-handler`, `Wrong response schema: ${error}`);
      throw error;
    },
  },
  handler: async (request: Request) => {
    const query = request.query as any;

    let baseQuery = `
        SELECT
          collection_id AS id,
          timestamp,
          volume_clean AS "volume",
          rank_clean AS "rank",
          floor_sell_value_clean AS "floor_sell_value",
          sales_count_clean AS "sales_count"                   
        FROM daily_volumes
      `;
    
    
    let newQuery = `SELECT DISTINCT ON (to_timestamp(dv.timestamp)::date)
    dv.collection_id,
    dv.timestamp,
    dv."volume",
    dv."rank",
    dv."floor_sell_value",
    dv."sales_count",
    er.day,
    er.valid_until,
    er.kind,
    er.contract,
    er.token_id,
    er.order_id,
    er.order_source_id_int,
    er.maker,
    er.price,
    er.previous_price,
    er.tx_hash,
    er.tx_timestamp,
    er.created_at,
    er.order_currency,
    er.order_currency_price,
    er.order_currency_normalized_value
  FROM daily_volumes dv
  LEFT JOIN (
    SELECT
      date_trunc('day', to_timestamp(events.tx_timestamp)) AS day,
      coalesce(
        nullif(date_part('epoch', upper(events.order_valid_between)), 'Infinity'),
        0
      ) AS valid_until,
      events.kind,
      events.collection_id,
      events.contract,
      events.token_id,
      events.order_id,
      events.order_source_id_int,
      events.maker,
      events.price,
      events.previous_price,
      events.tx_hash,
      to_timestamp(events.tx_timestamp) AS tx_timestamp,
      extract(epoch from events.created_at) AS created_at,
      order_currency,
      order_currency_price,
      order_currency_normalized_value,
      ROW_NUMBER() OVER (PARTITION BY date_trunc('day', to_timestamp(events.tx_timestamp)) ORDER BY events.price) AS row_num
    FROM collection_floor_sell_events events
    LEFT JOIN LATERAL (
      SELECT
        currency AS "order_currency",
        currency_price AS "order_currency_price",
        currency_normalized_value AS "order_currency_normalized_value"
      FROM orders
      WHERE events.order_id = orders.id
    ) o ON TRUE
    WHERE (to_timestamp(events.tx_timestamp) >= to_timestamp($/startTimestamp/))
      AND (to_timestamp(events.tx_timestamp) <= to_timestamp($/endTimestamp/))
      AND (coalesce(events.price, 0) >= 0)
      AND (events.collection_id = $/id/)
  ) AS er
  ON to_timestamp(dv.timestamp)::date = er.day
  ORDER BY to_timestamp(dv.timestamp)::date DESC`

    // baseQuery += ` WHERE collection_id = $/id/`;

    // We default in the code so that these values don't appear in the docs
    if (!query.startTimestamp) {
      query.startTimestamp = 0;
    }
    if (!query.endTimestamp) {
      query.endTimestamp = 9999999999;
    }

    // baseQuery += " AND timestamp >= $/startTimestamp/ AND timestamp <= $/endTimestamp/";

    // baseQuery += ` ORDER BY timestamp DESC`;

    newQuery += ` LIMIT $/limit/`;

    try {
      let result = await redb.manyOrNone(newQuery, query);
      result = result.map((r: any) => ({
        id: r.id,
        timestamp: r.timestamp,
        volume: formatEth(r.volume),
        rank: r.rank,
        floor_sell_value: formatEth(r.floor_sell_value),
        sales_count: r.sales_count,
        floor_price: r.price
      }));
      return { collections: result };
    } catch (error: any) {
      logger.error(`get-daily-volumes-${version}-handler`, `Handler failure: ${error}`);
      throw error;
    }
  },
};


/**
 * SELECT DISTINCT ON (to_timestamp(dv.timestamp)::date)
  dv.collection_id,
  dv.timestamp,
  dv."volume",
  dv."rank",
  dv."floor_sell_value",
  dv."sales_count",
  er.day,
  er.valid_until,
  er.kind,
  er.contract,
  er.token_id,
  er.order_id,
  er.order_source_id_int,
  er.maker,
  er.price,
  er.previous_price,
  er.tx_hash,
  er.tx_timestamp,
  er.created_at,
  er.order_currency,
  er.order_currency_price,
  er.order_currency_normalized_value
FROM daily_volumes dv
LEFT JOIN (
  SELECT
    date_trunc('day', to_timestamp(events.tx_timestamp)) AS day,
    coalesce(
      nullif(date_part('epoch', upper(events.order_valid_between)), 'Infinity'),
      0
    ) AS valid_until,
    events.kind,
    events.collection_id,
    events.contract,
    events.token_id,
    events.order_id,
    events.order_source_id_int,
    events.maker,
    events.price,
    events.previous_price,
    events.tx_hash,
    to_timestamp(events.tx_timestamp) AS tx_timestamp,
    extract(epoch from events.created_at) AS created_at,
    order_currency,
    order_currency_price,
    order_currency_normalized_value,
    ROW_NUMBER() OVER (PARTITION BY date_trunc('day', to_timestamp(events.tx_timestamp)) ORDER BY events.price) AS row_num
  FROM collection_floor_sell_events events
  LEFT JOIN LATERAL (
    SELECT
      currency AS "order_currency",
      currency_price AS "order_currency_price",
      currency_normalized_value AS "order_currency_normalized_value"
    FROM orders
    WHERE events.order_id = orders.id
  ) o ON TRUE
  WHERE (to_timestamp(events.tx_timestamp) >= to_timestamp(0))
    AND (to_timestamp(events.tx_timestamp) <= to_timestamp(9999999999))
    AND (coalesce(events.price, 0) >= 0)
    AND (events.collection_id = '0xabb3738f04dc2ec20f4ae4462c3d069d02ae045b')
) AS er
ON to_timestamp(dv.timestamp)::date = er.day
ORDER BY to_timestamp(dv.timestamp)::date DESC
 */
