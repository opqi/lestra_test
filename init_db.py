#!/usr/bin/env python

import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import sqlite3
from loguru import logger

uri = 'postgresql://lestra_user:lestra_pass@localhost:5432/lestra_analytics_db'

engine = create_engine(uri)


def init_db(dataset_file: str, table_name: str):
    logger.debug(f'Connect to sqlite3 {dataset_file}')
    conn = sqlite3.connect(dataset_file)

    query = f'SELECT * FROM {table_name}'

    df = pd.read_sql_query(query, conn)

    conn.close()

    logger.debug(f'Connect to postresql {dataset_file} and init {table_name}')
    pg_conn = engine.connect()

    df.to_sql(table_name, pg_conn,
              if_exists='append', index=False,
              chunksize=1000, method='multi')

    pg_conn.close()


def init_ships_db():

    dataset_file = 'data/Dataset-ships.db'
    lst_tables = ['arenas', 'arena_members', 'glossary_ships', 'catalog_items']

    for tn in lst_tables:
        init_db(dataset_file, tn)


def init_purchases_db():
    dataset_file = 'data/Dataset-purchases.db'
    lst_tables = ['packs_purchases']

    for tn in lst_tables:
        init_db(dataset_file, tn)


def daily_granulation():
    logger.debug(f'Connect to sqlite3')
    conn = sqlite3.connect('data/Dataset-ships.db')

    query_daily_granulation = '''SELECT
    DATE(start_dt) AS date,
    cat_type AS team_build_type,
    item_name AS ship_name,
    item_class AS ship_class,
    item_level AS ship_level,
    map_type_id,
        COUNT(*) AS TotalBattles,
        SUM(CASE WHEN Winner_team_id = 0 THEN 1 ELSE 0 END) AS Team1Wins,
        SUM(CASE WHEN Winner_team_id = 1 THEN 1 ELSE 0 END) AS Team2Wins,
        AVG(Duration_sec) AS AvgBattleDuration,
        SUM(Ships_killed) AS TotalShipsKilled,
        SUM(Damage) AS TotalDamageCaused,
        SUM(Received_damage) AS TotalReceivedDamage,
        SUM(Credits) AS TotalCreditsGained,
        SUM(Exp) AS TotalExperienceGained,
        1.0 * SUM(CASE WHEN Winner_team_id = Team_id THEN 1 ELSE 0 END) / COUNT(*) AS Winrate,
        1.0 * SUM(CASE WHEN Is_alive = 1 THEN 1 ELSE 0 END) / COUNT(*) AS Survivability,
        SUM(Damage) / COUNT(*) AS AvgDamagePerBattle,
        1.0 *SUM(Ships_killed) / NULLIF(SUM(CASE WHEN Received_damage > 0 THEN 1 ELSE 0 END), 0) AS FragsToDeathsRatio,
        AVG(Credits) AS AvgCreditsIncome,
        AVG(Exp) AS AvgExperienceIncome
    FROM arenas a
    JOIN arena_members m ON a.arena_id = m.arena_id
    JOIN glossary_ships g ON m.vehicle_type_id = g.item_cd
    JOIN catalog_items c ON CAST(c.cat_value as INT) = a.map_type_id
    GROUP BY date, team_build_type, ship_name, ship_class, ship_level, map_type_id;'''

    df_daily_gran = pd.read_sql_query(query_daily_granulation, conn)

    conn.close()

    logger.debug(f'Init daily granulation table')

    pg_conn = engine.connect()

    df_daily_gran.to_sql('daily_gran', pg_conn,
                         if_exists='append', index=False,
                         chunksize=1000, method='multi')

    pg_conn.close()


def packs_purchase_pattern():
    logger.debug(f'Connect to sqlite3')
    conn = sqlite3.connect('data/Dataset-purchases.db')

    query_pattern = '''SELECT
        pattern,
        COUNT(DISTINCT purchaser_id) AS user_count
    FROM (
        SELECT
            p1.purchaser_id,
            GROUP_CONCAT(p2.purchase, ' - ') AS pattern
        FROM packs_purchases p1
        LEFT JOIN packs_purchases p2 ON p1.purchaser_id = p2.purchaser_id AND p2.purchase_dt >= p1.purchase_dt
        GROUP BY p1.purchaser_id, p1.purchase_dt
    ) patterns
    GROUP BY pattern
    ORDER BY user_count DESC;
    '''

    df_pattern = pd.read_sql_query(query_pattern, conn)
    conn.close()

    logger.debug(f'Init packs purchase pattern table')

    pg_conn = engine.connect()

    df_pattern.to_sql('packs_purchase_pattern', pg_conn,
                         if_exists='append', index=False,
                         chunksize=1000, method='multi')

    pg_conn.close()


if __name__ == '__main__':
    # task 1
    init_ships_db()
    daily_granulation()

    # task 2
    init_purchases_db()
    packs_purchase_pattern()
