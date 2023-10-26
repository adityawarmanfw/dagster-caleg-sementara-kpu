import json
import asyncio
import aiohttp
import nest_asyncio
import pandas as pd
from bs4 import BeautifulSoup
import ssl

from dagster import asset, AssetExecutionContext, MetadataValue
from dagster_duckdb import DuckDBResource


@asset(compute_kind="python", group_name="caleg_sementara")
def raw_kode_dapil(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Get DataFrame from remote excel file.
    """
    ssl._create_default_https_context = ssl._create_unverified_context
    url = 'https://opendatadev.kpu.go.id/sites/default/files/files/469fd12d1fec74e91d301f56490f5da2.xls'
    kode_dapil_df = pd.read_excel(url)

    context.add_output_metadata(
        metadata={
            "num_records": len(kode_dapil_df),
            "preview": MetadataValue.md(kode_dapil_df.head().to_markdown()),
        }
    )
    return kode_dapil_df


@asset(compute_kind="duckdb", group_name="caleg_sementara")
def data_kode_dapil(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_kode_dapil: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform and returns DataFrame.
    """
    with duckdb.get_connection() as conn:
        conn.register("raw_kode_dapil", raw_kode_dapil)
        kode_dapil = conn.query(
            """
                SELECT DISTINCT
                    "kode dapil" AS kode_dapil,
                    "jenis dapil" AS jenis_dapil,
                    CASE "jenis dapil"
                            WHEN 'DPR RI' THEN 'dcs_dpr'
                            WHEN 'DPRD PROVINSI' THEN 'dcs_dprprov'
                            WHEN 'DPRD KABUPATEN/KOTA' THEN 'dcs_dprdkabko'
                    END AS url_dapil
                FROM raw_kode_dapil;
            """
        ).df()

        context.add_output_metadata(
            metadata={
                "num_records": len(kode_dapil),
                "preview": MetadataValue.md(kode_dapil.head().to_markdown()),
                "columns": MetadataValue.md(conn.execute("DESCRIBE SELECT * FROM kode_dapil").df().to_markdown()),
            }
        )
        return kode_dapil


@asset(compute_kind="duckdb", group_name="caleg_sementara")
def dapil_data_ids(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    data_kode_dapil: pd.DataFrame,
) -> pd.DataFrame:
    """
    Transform and returns DataFrame.
    """
    with duckdb.get_connection() as conn:
        conn.register("data_kode_dapil", data_kode_dapil)
        dapil_data_ids = conn.execute(
            """
                SELECT DISTINCT
                    CONCAT('dcs_dpr', '/', url_dapil, '?', 'kode_dapil=', kode_dapil) AS data_id
                FROM data_kode_dapil
                WHERE url_dapil = 'dcs_dpr'
                
                UNION ALL
                
                SELECT DISTINCT
                    CONCAT('dcs_dprprov', '/', url_dapil, '?', 'kode_dapil=', kode_dapil) AS data_id
                FROM data_kode_dapil
                WHERE url_dapil = 'dcs_dprprov'
                
                UNION ALL
                
                SELECT DISTINCT
                    CONCAT('dcs_dprd', '/', url_dapil, '?', 'kode_dapil=', kode_dapil) AS data_id
                FROM data_kode_dapil
                WHERE url_dapil = 'dcs_dprdkabko'
            """
        ).df()

        context.add_output_metadata(
            metadata={
                "num_records": len(dapil_data_ids),
                "preview": MetadataValue.md(dapil_data_ids.head().to_markdown()),
                "columns": MetadataValue.md(conn.execute("DESCRIBE SELECT * FROM dapil_data_ids").df().to_markdown()),
            }
        )
        return dapil_data_ids


@asset(compute_kind="python", group_name="jumlah_kursi")
def raw_kursi_dpr(context: AssetExecutionContext) -> pd.DataFrame:
    ssl._create_default_https_context = ssl._create_unverified_context
    url = 'https://infopemilu.kpu.go.id/Pemilu/Dapil_dpr'
    kursi_dapil_dpr = pd.read_html(url, flavor='bs4')
    kursi_dapil_dpr_df = pd.DataFrame(kursi_dapil_dpr[0])

    context.add_output_metadata(
        metadata={
            "num_records": len(kursi_dapil_dpr_df),
            "preview": MetadataValue.md(kursi_dapil_dpr_df.head().to_markdown()),
        }
    )

    return kursi_dapil_dpr_df


@asset(compute_kind="python", group_name="jumlah_kursi")
def raw_kursi_dprd_prov(context: AssetExecutionContext) -> pd.DataFrame:
    ssl._create_default_https_context = ssl._create_unverified_context
    url = 'https://infopemilu.kpu.go.id/Pemilu/Dapil_dpr_prov'
    kursi_dprd_prov = pd.read_html(url, flavor='bs4')
    kursi_dprd_prov_df = pd.DataFrame(kursi_dprd_prov[0])

    context.add_output_metadata(
        metadata={
            "num_records": len(kursi_dprd_prov_df),
            "preview": MetadataValue.md(kursi_dprd_prov_df.head().to_markdown()),
        }
    )

    return kursi_dprd_prov_df


@asset(compute_kind="python", group_name="jumlah_kursi")
def raw_kursi_dprd_kabko(context: AssetExecutionContext) -> pd.DataFrame:
    ssl._create_default_https_context = ssl._create_unverified_context
    url = 'https://opendatadev.kpu.go.id/sites/default/files/files/c0e38c0d5c65e19def6ce9d94b3157ad.xlsx'
    kursi_dprd_kabko_df = pd.read_excel(url)

    context.add_output_metadata(
        metadata={
            "num_records": len(kursi_dprd_kabko_df),
            "preview": MetadataValue.md(kursi_dprd_kabko_df.head().to_markdown())
        }
    )

    return kursi_dprd_kabko_df


@asset(compute_kind="duckdb", group_name="jumlah_kursi")
def kursi_dapil(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_kursi_dpr: pd.DataFrame,
    raw_kursi_dprd_prov: pd.DataFrame,
    raw_kursi_dprd_kabko: pd.DataFrame
) -> pd.DataFrame:

    with duckdb.get_connection() as conn:
        conn.register("raw_kursi_dpr", raw_kursi_dpr)
        conn.register("raw_kursi_dprd_prov", raw_kursi_dprd_prov)
        conn.register("raw_kursi_dprd_kabko", raw_kursi_dprd_kabko)
        kursi_dapil_dpr = conn.query(
            '''
                SELECT
                    'DPR RI' AS jenis_dapil,
                    TRIM(dapil) AS dapil,
                    "JUMLAH KURSI"::INT AS jumlah_kursi
                FROM raw_kursi_dpr
                WHERE "JUMLAH KURSI" IS NOT NULL
            ''').df()

        kursi_dapil_dprd_prov = conn.query(
            '''
                SELECT
                    'DPRD PROVINSI' AS jenis_dapil,
                    TRIM(dapil) AS dapil,
                    "JUMLAH KURSI"::INT AS jumlah_kursi
                FROM raw_kursi_dprd_prov
                WHERE "JUMLAH KURSI" IS NOT NULL
            ''').df()

        kursi_dapil_dprd_kabko = conn.execute(
            '''
                    WITH stg AS (
                        SELECT
                            "KODE KABUPATEN/ KOTA"::INT AS kode_kabko,
                            TRIM("KABUPATEN/KOTA") AS nama_kabko,
                            "Alokasi Kursi Dapil 1"::INT AS "1",
                            "Alokasi Kursi Dapil 2"::INT AS "2",
                            "Alokasi Kursi Dapil 3"::INT AS "3",
                            "Alokasi Kursi Dapil 4"::INT AS "4",
                            "Alokasi Kursi Dapil 5"::INT AS "5",
                            "Alokasi Kursi Dapil 6"::INT AS "6",
                            "Alokasi Kursi Dapil 7"::INT AS "7",
                            "Alokasi Kursi Dapil 8"::INT AS "8",
                            "Alokasi Kursi Dapil 9"::INT AS "9"
                        FROM raw_kursi_dprd_kabko
                        WHERE PROVINSI != 'Menampilkan 1 sampai 508 dari 508 entri'
                    ), unpivoted AS (
                        UNPIVOT stg
                        ON COLUMNS(* EXCLUDE (kode_kabko, nama_kabko))
                        INTO
                            NAME dapil
                            VALUE jumlah_kursi
                    )
                    SELECT
                        'DPRD KABUPATEN/KOTA' AS jenis_dapil,
                        REGEXP_REPLACE(nama_kabko, 'KAB. |KOTA ', '') || ' ' || dapil,
                        jumlah_kursi
                    FROM unpivoted
            ''').df()

        kursi_dapil = conn.query(
            '''
                SELECT * FROM kursi_dapil_dpr
                UNION ALL
                SELECT * FROM kursi_dapil_dprd_prov
                UNION ALL
                SELECT * FROM kursi_dapil_dprd_kabko
            ''').df()

        conn.execute("COPY kursi_dapil TO './outputs/kursi_dapil.parquet' (FORMAT PARQUET)")
        context.log.info("Exported to parquet.")

        context.add_output_metadata(
            metadata={
                "num_records": len(kursi_dapil),
                "preview": MetadataValue.md(kursi_dapil.head().to_markdown()),
                "columns": MetadataValue.md(conn.execute("DESCRIBE SELECT * FROM kursi_dapil").df().to_markdown()),
            }
        )
        return kursi_dapil


async def fetch_data(session, id, semaphore):
    base_url = "https://infopemilu.kpu.go.id/Pemilu/"
    async with semaphore:
        try:
            async with session.get(base_url + id, verify_ssl=False) as response:
                response.raise_for_status()
                if response.status == 200:
                    data = await response.text()
                    json_data = json.loads(data)
                    print(f"{id} fetched.")
                    return (id, json_data['data'])
                else:
                    print(f"Error fetching data, resp != 200")
        except (aiohttp.ClientError, ValueError) as e:
            print(f"Error fetching data {e}")


async def fetch_all_data(data_ids, num_workers):
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(num_workers)
        tasks = [asyncio.create_task(fetch_data(
            session, id, semaphore=semaphore)) for id in data_ids]
        fetched_data = await asyncio.gather(*tasks)
        return fetched_data


def extract_data_from_html(html_data):
    soup = BeautifulSoup(html_data, 'lxml')
    text_elements = soup.find_all(string=True)
    return text_elements


def process_fetched_data(id, data_list):
    results = []
    for data_set in data_list:
        extracted_data = [extract_data_from_html(
            html_data) for html_data in data_set]
        results.append({
            "id": id,
            "partai": extracted_data[0][0].strip(),
            "dapil": extracted_data[1][1].strip(),
            "no_urut": int(extracted_data[2][1]),
            "nama": extracted_data[4][0].strip(),
            "jenis_kelamin": extracted_data[5][0].strip()
        })
    return results


async def get_data(data_ids):
    fetched_data = await fetch_all_data(data_ids, num_workers=10)
    final_results = []
    for data_item in fetched_data:
        if data_item is not None:
            id, data_list = data_item
            if all(data_list):
                final_results.extend(process_fetched_data(id, data_list))
    return final_results


@asset(compute_kind="python", group_name="caleg_sementara")
async def raw_daftar_caleg_sementara(context: AssetExecutionContext, dapil_data_ids: pd.DataFrame) -> pd.DataFrame:
    nest_asyncio.apply()
    result = await get_data(dapil_data_ids['data_id'])
    result_df = pd.DataFrame(result)

    context.add_output_metadata(
        metadata={
            "num_records": len(result_df),
            "preview": MetadataValue.md(result_df.head().to_markdown()),
        }
    )

    context.log.warn(f"Number of unique dapil values: {result_df['dapil'].nunique()}")
    context.log.warn(f"Expected unique dapil values: {len(dapil_data_ids)}")
    context.log.warn(f"Diff: {result_df['dapil'].nunique() - len(dapil_data_ids)}")

    return result_df


@asset(compute_kind="duckdb", group_name="caleg_sementara")
def daftar_caleg_sementara(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    raw_daftar_caleg_sementara: pd.DataFrame
) -> pd.DataFrame:
    with duckdb.get_connection() as conn:
        conn.register("raw_daftar_caleg_sementara", raw_daftar_caleg_sementara)
        df = conn.execute(
            """
                SELECT
                UPPER(partai) AS partai,
                CASE
                    WHEN id LIKE '%dprdkabko%' THEN 'DPRD KABUPATEN/KOTA'
                    WHEN id LIKE '%dcs_dprprov%' THEN 'DPRD PROVINSI'
                    ELSE 'DPR RI' END AS jenis_dapil,
                dapil,
                no_urut,
                nama,
                IF(jenis_kelamin = 'LAKI - LAKI', 'LAKI-LAKI', jenis_kelamin) AS jenis_kelamin
                FROM raw_daftar_caleg_sementara;
            """).df()

        conn.execute(
            "COPY df TO './outputs/daftar_caleg_sementara.parquet' (FORMAT PARQUET)")
        context.log.info("Exported to parquet.")

        context.add_output_metadata(
            metadata={
                "num_records": len(df),
                "preview": MetadataValue.md(df.head().to_markdown()),
                "columns": MetadataValue.md(conn.execute("DESCRIBE SELECT * FROM df").df().to_markdown()),
            }
        )
        return df


@asset(compute_kind="duckdb", group_name="analysis")
def keterwakilan_perempuan(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    kursi_dapil: pd.DataFrame,
    daftar_caleg_sementara: pd.DataFrame
) -> pd.DataFrame:
    with duckdb.get_connection() as conn:
        conn.register("kursi_dapil", kursi_dapil)
        conn.register("daftar_caleg_sementara", daftar_caleg_sementara)
        df = conn.execute(
            """
                WITH dcs AS (
                    SELECT 
                        partai,
                        jenis_dapil,
                        REGEXP_REPLACE(dapil, 'KOTA |KAB ', '') AS dapil,
                        COUNT(no_urut)::INT AS jumlah_calon,
                        COUNT_IF(jenis_kelamin = 'LAKI-LAKI')::INT AS laki_laki,
                        COUNT_IF(jenis_kelamin = 'PEREMPUAN')::INT AS perempuan
                    FROM daftar_caleg_sementara
                    GROUP BY 1,2,3
                    )
                SELECT 
                    *,
                    jumlah_calon/jumlah_kursi AS persentase_calon,
                    ROUND(jumlah_calon::DOUBLE*0.3, 2) AS keterwakilan_perempuan,
                    CEIL(jumlah_calon::DOUBLE*0.3) AS keterwakilan_perempuan_bulat,
                    FLOOR(perempuan/keterwakilan_perempuan_bulat) AS status_keterwakilan_perempuan
                FROM dcs
                LEFT JOIN kursi_dapil USING (jenis_dapil, dapil)
            """).df()
        
        conn.execute(
            "COPY df TO './outputs/keterwakilan_perempuan.parquet' (FORMAT PARQUET)")
        context.log.info("Exported to parquet.")

        context.add_output_metadata(
            metadata={
                "num_records": len(df),
                "preview": MetadataValue.md(df.head().to_markdown()),
                "columns": MetadataValue.md(conn.execute("DESCRIBE SELECT * FROM df").df().to_markdown()),
            }
        )
        return df