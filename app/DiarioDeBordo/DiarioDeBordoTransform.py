from pyspark.sql.functions import col, count, when, sum, max, min, avg, to_timestamp, date_format
from DiarioDeBordoEnum import DiarioDeBordoEnum

class DiarioDeBordoTransform:

    def __init__(self, spark):
    
        self.spark = spark

    def validandoDados(self, df_bronze_bruta):

        df_dados_validos = (
            df_bronze_bruta
            .filter(
                col('CATEGORIA').isNotNull() &
                col('PROPOSITO').isNotNull() &
                col('DISTANCIA').isNotNull() &
                col('DATA_INICIO').isNotNull()
            ) 
        )

        df_dados_invalidos = (
            df_bronze_bruta
            .subtract(df_dados_validos)
        )

        return df_dados_validos, df_dados_invalidos


    def transformandoColunasData(self, df_bronze_bruta):

        df_bronze_data_tratada = (
            df_bronze_bruta.select(
                col('CATEGORIA'),
                col('PROPOSITO'),
                col('DISTANCIA'),
                date_format(
                    to_timestamp(col('DATA_INICIO'), 'MM-dd-yyyy HH:mm'), 
                    'yyyy-MM-dd'
                ).alias('dt_refe')
            )
        )

        return df_bronze_data_tratada

    def agrupandoDadosPorData(self, df_bronze_data_tratada):

        df_agrupado_por_data = (
            df_bronze_data_tratada
            .groupBy(
                col('dt_refe')
            ).agg(
                count('*').alias('qt_corr'),

                sum(
                    when( 
                        col('categoria') == 'Negocio', 1
                    ).otherwise(0)
                ).alias('qt_corr_neg'),

                sum(
                    when( 
                        col('categoria') == 'Pessoal', 1
                    ).otherwise(0)
                ).alias('qt_corr_pess'),

                max(
                    col('DISTANCIA')
                ).alias('vl_max_dist'),

                min(
                    col('DISTANCIA')
                ).alias('vl_min_dist'),

                avg(
                    col('DISTANCIA')
                ).alias('vl_avg_dist'),

                sum(
                    when(
                        col('PROPOSITO') == 'Reunião', 1
                    ).otherwise(0)
                ).alias('qt_corr_reuni'),

                sum(
                    when(
                        col('PROPOSITO') != 'Reunião', 1
                    ).otherwise(0)
                ).alias('qt_corr_nao_reuni')
            )
        )

        return df_agrupado_por_data

    def inserindoDadosNaQuarentena(self, df_bronze_validado):

        (
            df_bronze_validado.write
            .mode('overwrite')
            .format('delta')
            .saveAsTable(
                f'{DiarioDeBordoEnum.CatalogoSilver}.'
                f'{DiarioDeBordoEnum.TabelaQuarentena}'
            )
        )


    def salvandoDadosSilver(self, df_dados_agrupados):

        (
            df_dados_agrupados.write
            .mode('overwrite')
            .format('delta')
            .partitionBy('dt_refe')
            .saveAsTable(
                f'{DiarioDeBordoEnum.CatalogoSilver}.'
                f'{DiarioDeBordoEnum.TabelaTransporte}'
                )
        )


