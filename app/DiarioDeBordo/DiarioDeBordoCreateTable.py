class DiarioDeBordoCreateTable:
    def __init__(self, spark):
        self.spark = spark

    def criar_schema_silver(self):
        self.spark.sql("""
            CREATE SCHEMA IF NOT EXISTS workspace.silver
        """)

    def criar_tabela_transporte(self):
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS workspace.silver.transporte (
                dt_refe STRING,
                qt_corr BIGINT,
                qt_corr_neg BIGINT,
                qt_corr_pess BIGINT,
                vl_max_dist STRING,
                vl_min_dist STRING,
                vl_avg_dist DOUBLE,
                qt_corr_reuni BIGINT,
                qt_corr_nao_reuni BIGINT
            )
            USING DELTA
            PARTITIONED BY (dt_refe)
        """)

    def criar_tabela_transporte_quarentena(self):
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS workspace.silver.quarentena_transporte (
                data_inicio Timestamp,
                data_fim Timestamp,
                categoria STRING,
                local_inicio STRING,
                local_fim STRING,
                distancia STRING,
                proposito STRING
            )
            USING DELTA
        """)


    def executar(self):
        self.criar_schema_silver()
        self.criar_tabela_transporte()
        self.criar_tabela_transporte_quarentena()
