import sys
sys.path.append('/Workspace/Users/lea.malachini@gmail.com/diario_de_bordo')

from service.LoggerService import LoggerService
from app.DiarioDeBordo.DiarioDeBordoEnum import DiarioDeBordoEnum
from app.DiarioDeBordo.DiarioDeBordoTransform import DiarioDeBordoTransform
from app.DiarioDeBordo.DiarioDeBordoCreateTable import DiarioDeBordoCreateTable
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

class DiarioDeBordoExecute:

    def __init__(self, spark):
        
        self.spark = spark
        
        self.schema = StructType([
            StructField("data_inicio", TimestampType(), True),
            StructField("data_fim", TimestampType(), True),
            StructField("categoria", StringType(), True),
            StructField("local_inicio", StringType(), True),
            StructField("local_fim", StringType(), True),
            StructField("distancia", StringType(), True),
            StructField("proposito", StringType(), True)
        ])

        self.DiarioDeBordoTransform = DiarioDeBordoTransform(self.spark)

        self.log = (
            LoggerService('DiarioDeBordo').logger
        )

    def run(self):

        self.log.info('Lendo os dados brutos de transporte.')
        df_dados_bronze_transportes = (
            spark.read
                .format('csv')
                .option("header", True)
                .option("timestampFormat", "MM-dd-yyyy HH:mm")
                .option("delimiter", ";")
                .schema(self.schema)
                .load(DiarioDeBordoEnum.CaminhoDadosBronze)
        )

        self.log.info('Validando os dados brutos recebidos.')
        df_dados_bronze_subtract = (
            self.DiarioDeBordoTransform.
                validandoDados(df_dados_bronze_transportes)
        )

        qtd_dados_invalidos = df_dados_bronze_subtract[1].count()

        if qtd_dados_invalidos > 0:
            self.log.info(f'Qtd de dados invalidos {qtd_dados_invalidos}')
            self.log.info('Salvando dados invalidos na Quarentena')
            (
                self.DiarioDeBordoTransform
                    .inserindoDadosNaQuarentena(df_dados_bronze_subtract[1])
            )

        self.log.info('Ajustando o formato da coluna de data.')
        df_bronze_data_tratada = (
            self.DiarioDeBordoTransform.
                transformandoColunasData(df_dados_bronze_subtract[0])
        )

        self.log.info('Agregando todas as informacoes por data.')
        df_dados_arrumados = (
            self.DiarioDeBordoTransform.
                agrupandoDadosPorData(df_bronze_data_tratada)
        )

        self.log.info('Salvando os dados processados na tabela Silver.')
        self.DiarioDeBordoTransform.salvandoDadosSilver(df_dados_arrumados)


if __name__ == "__main__":

    DiarioDeBordoCreateTable(spark).executar()

    pipeline_diario_bordo_silver =(
        DiarioDeBordoExecute(spark)
    )

    pipeline_diario_bordo_silver.run()

