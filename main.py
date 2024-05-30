import pandas as pd
import matplotlib as plt
import plotly.express as px
from reportlab.lib import colors
from reportlab.pdfbase.ttfonts import TTFont
from src import pngs, processamento

#Variaveis
stock_report_vispera = './data/sem21_projeto-sococo_stock_report_weekly_May_2024.csv'
base_email = 'data/base_e-mails_sococo.xlsx'
lista_skus = 'data/LISTA_SKUs.xlsx' 

#Limpeza e Processamento
processamento.processar_dados(stock_report_vispera, base_email, lista_skus)
pngs.create_pngs()