import os
import tempfile
import pandas as pd
import plotly.graph_objects as go
from PIL import Image
from datetime import datetime, timedelta
from unidecode import unidecode
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4

image_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))+r"\assets\Imagem1.png"

#Tratando as datas.
data_atual = datetime.now()
while data_atual.weekday() != 0:
    data_atual = data_atual - timedelta(days=1)    
data_prev = data_atual - timedelta(weeks=2)
data_last = data_atual - timedelta(weeks=1)
data_atual = data_atual.strftime("%d_%m_%Y")
data_prev = data_prev.strftime("%d_%m_%Y")
data_last = data_last.strftime("%d_%m_%Y")


#Persistindo a base
df_persistent = pd.DataFrame()

def processar_dados(stock_report_vispera, base_email, lista_skus):
    global df_persistent
    df_stock_report = pd.read_csv(stock_report_vispera, sep=';')
    
    df_stock_report = df_stock_report.sort_values('StartedAt').groupby(['StoreCode', 'StoreName']).last().reset_index()
    df_stock_report['StartedAt'] = pd.to_datetime(df_stock_report['StartedAt'], utc=True)
    df_stock_report['semana_iso'] = df_stock_report['StartedAt'].dt.isocalendar().week
    df_stock_report['StartedAt'] = df_stock_report['StartedAt'].dt.tz_localize(None)

    colunas_fixas = ['VisitID', 'StartedAt', 'semana_iso', 'StoreID', 'StoreCode', 'StoreName', 'AuditorID', 'AuditorUsername']
    colunas_para_transpor = [col for col in df_stock_report.columns if col not in colunas_fixas]
    
    df_melted_stock_report = df_stock_report.melt(id_vars=['VisitID', 'StartedAt', 'semana_iso', 'StoreCode', 'StoreName', 'AuditorUsername'], value_vars=colunas_para_transpor)
    
    df_melted_stock_report.rename({'variable':'SKU', 'StoreCode':'Store Code', 'StoreName':'Store Name'}, axis=1, inplace=True)
    
    df_melted_stock_report = df_melted_stock_report.sort_values(by=['VisitID', 'Store Name', 'AuditorUsername'])
    
    
    #EXECUTIVOS
    df_executivos = pd.read_excel(base_email)
    df_executivos["Store Code"] = df_executivos["Store Code"].astype(str)
    df_executivos['Nome_Vend'] = df_executivos['Nome_Vend'].astype(str).str.capitalize().apply(unidecode)
    df_executivos['Nome_GT'] = df_executivos['Nome_GT'].astype(str).str.capitalize().apply(unidecode)
    df_executivos['Nome_CM'] = df_executivos['Nome_CM'].astype(str).str.capitalize().apply(unidecode)
    df_executivos.rename({"Nome":"Nome_loja","E-mail":"e-mail_vend", "E-mail.1":"e-mail_gc", "E-mail.2":"e-mail_gt", "E-mail.3":"e-mail_cm"}, axis=1, inplace=True)

    df_sococo = pd.merge(df_melted_stock_report, df_executivos, on="Store Code", how="left")
    df_sococo.rename({'Visit ID':'VisitID'}, axis=1, inplace=True)
    
    df_master_skus = pd.read_excel('data/SkusMasterData_export_rt_vendas_project_29_January_19_49.xlsx')
    df_master_skus.rename({'display_name':'SKU'}, axis=1, inplace=True)
    df_master_skus = df_master_skus[['SKU', 'barcode']]
    
    df_mix_obrigatorio = pd.read_excel(lista_skus)
    df_mix_obrigatorio.rename({'NOME DO SKU - VISPERA':'SKU','CODIGO DE BARRAS':'SKU Barcode'}, axis=1, inplace=True)
    
    lista_executivos = df_sococo['Nome_Vend'].unique()
    
    pasta_relatorios_vend = 'report_vendedores'

    if not os.path.exists(pasta_relatorios_vend):
        os.makedirs(pasta_relatorios_vend)

#PDF

    # Abrir a imagem para obter as dimensões originais
    with Image.open(image_path) as img:
        original_width, original_height = img.size

    # Definir a largura ou altura desejada
    desired_width = 115  # ou a altura desejada

    # Calcular a nova altura mantendo a proporção
    scale_ratio = desired_width / original_width
    new_height = original_height * scale_ratio


    for executivo in lista_executivos:
        ############### MANIPULANDO DADOS
        df_executivo_filtered = df_sococo[df_sococo['Nome_Vend']==executivo]
        df_executivo_filtered_b = df_sococo[df_sococo['Nome_Vend']==executivo]

        new_table_ruptura = df_executivo_filtered

        new_table_ruptura = pd.merge(new_table_ruptura, df_master_skus, how='left', on='SKU')
        new_table_ruptura = new_table_ruptura[['VisitID','Store Code', 'Store Name',
            'StartedAt', 'semana_iso',
            'Cod_Vend', 'Nome_Vend', 'e-mail_vend', 'Nome_GC',
            'e-mail_gc', 'Nome_GT', 'e-mail_gt', 'Nome_CM','e-mail_cm',
            'SKU', 'value', 'barcode'
            ]]

        new_table_ruptura = pd.merge(new_table_ruptura, df_mix_obrigatorio, left_on=['SKU', 'barcode'], right_on=['SKU', 'SKU Barcode'])

        new_table_ruptura.sort_values(by=['VisitID', 'Store Code', 'Store Name'], inplace=True)
        new_table_ruptura['value'] = new_table_ruptura.apply(lambda x: 'Presente' if x['value']==1 else 'Ruptura', axis=1)

        #att 9jan
        new_table_ruptura = new_table_ruptura.drop('barcode', axis=1)

        new_table_ruptura = new_table_ruptura.rename({'VisitID':'ID_VISITA',
                                            'Store Code':'COD_LOJA',
                                            'Store Name':'NOME_LOJA',
                                            'StartedAt':'DATA_INICIO_PESQUISA',
                                            'value':'DISPONIBILIDADE',
                                            'SKU Barcode':'SKU_COD_BARRA'
                                            }, axis=1)
        #
        teste = new_table_ruptura.groupby(['ID_VISITA','COD_LOJA','NOME_LOJA','DISPONIBILIDADE']).agg({'DISPONIBILIDADE':'count'})
        teste.rename({'DISPONIBILIDADE':'Contagem'},axis=1, inplace=True)
        teste = teste.reset_index()

        # Transformando os valores da coluna 'DISPONIBILIDADE' em colunas separadas
        df_transformed = teste.pivot_table(index=['ID_VISITA','COD_LOJA', 'NOME_LOJA'], 
                                        columns='DISPONIBILIDADE', 
                                        values='Contagem', 
                                        fill_value=0).reset_index()

        if 'Presente' not in df_transformed.columns:
            df_transformed['Presente'] = 0

        if 'Ruptura' not in df_transformed.columns:
            df_transformed['Ruptura'] = 0

        # Renomeando as colunas para corresponder ao pedido
        df_transformed.columns.name = None  # Remover o nome da categoria de colunas
        df_transformed.rename(columns={'Presente': 'Presente', 'Ruptura': 'Ruptura'}, inplace=True)

        df_transformed['Total'] = df_transformed['Presente'] + df_transformed['Ruptura']
        df_transformed['Mix Obrigatório'] = round(((df_transformed['Presente'] / df_transformed['Total'])*100), 1)

        df_transformed['Presente'] = df_transformed['Presente'].astype(int)
        df_transformed['Ruptura'] = df_transformed['Ruptura'].astype(int)
        df_transformed['Total'] = df_transformed['Total'].astype(int)

        df_transformed = df_transformed.sort_values(by='Mix Obrigatório', ascending=True)

        df_transformed['Mix Obrigatório'] = df_transformed.apply(lambda x: 100 if x['Mix Obrigatório']>=100 else x['Mix Obrigatório'], axis=1)


        df_transformed.drop('ID_VISITA', axis=1, inplace=True)
        df_transformed["Date"] = data_atual
        df_transformed["Executivo"] = executivo
        df_persistent = pd.concat([df_persistent, df_transformed], ignore_index=True)
        #df_transformed.drop(columns=['Date', 'Executivo'], inplace=True)
        mean_mix = df_transformed["Mix Obrigatório"].mean()
        df_transformed.rename({'Mix Obrigatório':'Mix'},axis=1, inplace=True)


        #!
        dfp = pd.read_excel("Final_"+data_prev.replace("/", "_")+".xlsx")
        dfp["COD_LOJA"] = dfp["COD_LOJA"].astype(str)
        df_juntado = pd.merge(df_transformed, dfp, on = ["COD_LOJA"], how="outer")
        
        dfp = pd.read_excel("Final_"+data_last.replace("/", "_")+".xlsx")
        dfp["COD_LOJA"] = dfp["COD_LOJA"].astype(str)
        df_juntado = pd.merge(df_juntado, dfp, on = ["COD_LOJA"], how="outer")
        

        #asd
        df_transformed = pd.merge(df_transformed, df_juntado[['NOME_LOJA', 'Mix Obrigatório_x', 'Mix Obrigatório_y']], on='NOME_LOJA', how='outer')
        cols = ["COD_LOJA", "NOME_LOJA", "Presente", "Ruptura", "Total", "Mix Obrigatório_x", "Mix Obrigatório_y", "Mix"]
        df_transformed = df_transformed[cols]
        df_transformed.drop(columns=['Total'], inplace=True)
        df_transformed = df_transformed.dropna(subset=['Mix'])
        df_mean = df_transformed.copy()
        df_transformed["Mix"] = df_transformed["Mix"].astype("str")+"%"
        df_transformed['Mix Obrigatório_x'] = df_transformed['Mix Obrigatório_x'].astype('str')+'%'
        df_transformed['Mix Obrigatório_x'] = df_transformed['Mix Obrigatório_x'].replace('nan%', '-')
        df_transformed['Mix Obrigatório_y'] = df_transformed['Mix Obrigatório_y'].astype('str')+'%'
        df_transformed['Mix Obrigatório_y'] = df_transformed['Mix Obrigatório_y'].replace('nan%', '-')
        df_transformed.rename(columns={'Mix Obrigatório_x': data_prev[:5].replace("_", "/"), 'Mix Obrigatório_y': data_last[:5].replace("_", "/"), "Mix": data_atual[:5].replace("_", "/")}, inplace=True)



        #Dados para tabela
        table_gc = df_transformed.iloc[0:30]
        table_gc_2 = df_transformed.iloc[30:50]
        mean_prev = df_mean['Mix Obrigatório_x'].astype("float").mean()
        mean_last = df_mean['Mix Obrigatório_y'].astype("float").mean()
        df_mean["Mean-2"] = mean_prev
        df_mean["Mean-1"] = mean_last
        df_mean["Mean"] = mean_mix
        df_mean["Mean-2"] = df_mean['Mean-2'].round(1).astype("str")+"%"
        df_mean["Mean-1"] = df_mean['Mean-1'].round(1).astype("str")+"%"
        df_mean["Mean"] = df_mean['Mean'].round(1).astype("str")+"%"
        df_mean.drop(columns=["Presente", "NOME_LOJA", "COD_LOJA"], inplace=True)
        table_gc_3 = df_mean.iloc[0:1]


        
        #table_gc = df_gc_filtered.copy()

        # grafico table
        header = list(table_gc.columns)
        cell_values1 = [table_gc[col].tolist() for col in table_gc.columns]

        # Altura para o cabeçalho e células
        header_height = 30
        cell_height = 30


        # Criando a tabela com gráficos de barra
        fig_table_gc = go.Figure(data=[go.Table(
            columnwidth = [200, 900, 250, 250, 250, 250],
            header=dict(
                values=header,
                align='center',
                font_size=25,
                font_color='#FFFFFF',
                height=header_height,
                fill_color='#C01614'
            ),
            cells=dict(
                values=cell_values1,
                align='center',
                font_size=25,
                font_color=[['#707070'],['#707070'],['#707070'],['#707070'],['#707070'],['#707070'],['#FFFFFF']],
                height=cell_height,
                fill_color=[['#F4EDB2']*(len(table_gc)),['#F4EDB2'],['#F4EDB2'],['#F4EDB2'],['#F4EDB2'],['#F4EDB2'],['#777777']]*len(table_gc.columns)
            )
        )])
        fig_table_gc.update_layout(height=1200,width=2100,margin=dict(l=0, r=0, t=0, b=0))

        temp_file_gc_table = tempfile.mktemp(suffix=".png")
        fig_table_gc.write_image(temp_file_gc_table)

        # grafico table 2
        header = list(table_gc_2.columns)
        cell_values2 = [table_gc_2[col].tolist() for col in table_gc_2.columns]
        table_size = len(cell_values1[1])+len(cell_values2[1])

        # Altura para o cabeçalho e células
        header_height = 0
        cell_height = 30



        # Criando a tabela com gráficos de barra 2
        fig_table_gc_2 = go.Figure(data=[go.Table(
            columnwidth = [200, 900, 250, 250, 250, 250],
            header=dict(
                values=[""]*len(cell_values2),
                height=0
            ),
            cells=dict(
                values=cell_values2,
                align='center',
                font_size=25,
                font_color=[['#707070'],['#707070'],['#707070'],['#707070'],['#707070'],['#707070'],['#FFFFFF']],
                height=cell_height,
                fill_color=[['#F4EDB2'],['#F4EDB2'],['#F4EDB2'],['#F4EDB2'],['#F4EDB2'],['#F4EDB2'],['#777777']]*len(table_gc.columns)
            )
        )])
        fig_table_gc_2.update_layout(height=1200,width=2100,margin=dict(l=0, r=0, t=0, b=0))

        temp_file_gc_table2 = tempfile.mktemp(suffix=".png")
        fig_table_gc_2.write_image(temp_file_gc_table2)



        # grafico table 2
        header = list(table_gc_3.columns)
        cell_values3 = [table_gc_3[col].tolist() for col in table_gc_3.columns]


        # Altura para o cabeçalho e células
        header_height = 0
        cell_height = 30
        cell_values3[3] = "Média"


        # Criando a tabela com gráficos de barra 3
        fig_table_gc_3 = go.Figure(data=[go.Table(
            columnwidth = [200, 900, 250, 250, 250, 250],
            header=dict(
                values=[""]*len(cell_values3),
                height=0
            ),
            cells=dict(
                values=cell_values3,
                align='center',
                font_size=25,
                font_color=[['#FFFFFF'],['#FFFFFF'],['#FFFFFF'],['#FFFFFF'],['#707070'],['#707070'],['#FFFFFF']],
                height=cell_height,
                fill_color=[['#FFFFFF'],['#FFFFFF'],['#FFFFFF'],['#FFFFFF'],['#e6dea1'],['#e6dea1'],['#575757']]*len(table_gc.columns)
            )
        )])
        fig_table_gc_3.update_layout(height=1200,width=2100,margin=dict(l=0, r=0, t=0, b=0))

        temp_file_gc_table3 = tempfile.mktemp(suffix=".png")
        fig_table_gc_3.write_image(temp_file_gc_table3)



    ############### ESCREVENDO PDF
        cnv = canvas.Canvas(os.path.join(pasta_relatorios_vend, f'relatorio_{executivo}_{data_atual}.pdf'), pagesize=A4)

        #topbar layout
        rgb_color = (0.8862745098039215, 0.16862745098039217, 0.1607843137254902)
        cnv.setStrokeColorRGB(*rgb_color)
        cnv.setLineWidth(100)
        cnv.line(x1=(60/0.352777), y1=(297/0.352777), x2=(210/0.352777), y2=(297/0.352777))

        #logo_raymundo
        cnv.drawImage(image_path, 30, 805, width=desired_width, height=new_height,mask='auto')

        #logo_RT
        image_path_rt = 'assets/imagem_logo.png'
        # Abrir a imagem para obter as dimensões originais
        with Image.open(image_path_rt) as img:
            original_width_3, original_height_3 = img.size
        # Definir a largura ou altura desejada
        desired_width_3 = 80  # ou a altura desejada
        # Calcular a nova altura mantendo a proporção
        scale_ratio_3 = desired_width_3 / original_width_3
        new_height_3 = original_height_3 * scale_ratio_3

        cnv.drawImage(image_path_rt, 510, 765, width=desired_width_3, height=new_height_3,mask='auto')


        #logo_VISPERA
        image_path_vispera = 'assets/vispera.png'
        # Abrir a imagem para obter as dimensões originais
        with Image.open(image_path_vispera) as img:
            original_width_2, original_height_2 = img.size
        # Definir a largura ou altura desejada
        desired_width_2 = 70  # ou a altura desejada
        # Calcular a nova altura mantendo a proporção
        scale_ratio_2 = desired_width_2 / original_width_2
        new_height_2 = original_height_2 * scale_ratio_2

        cnv.drawImage(image_path_vispera, 510, 10, width=desired_width_2, height=new_height_2,mask='auto')


        cnv.setFillColorRGB(0,0,0)


        rgb_color_nomes = (0.8627450980392157, 0, 0.06274509803921569)
        # Escrevendo a Olá Romero
        cnv.setFont("Helvetica-Bold", 12)  # Exemplo com tamanho de fonte 12
        cnv.setFillColorRGB(*rgb_color_nomes)
        cnv.drawString(5, 757, f"{executivo}")

        # frase abaixo da boa-vinda
        cnv.setFont("Helvetica", 10)
        cnv.drawString(5, 740, "Segue abaixo relatório consolidado:")

        cnv.setFont("Helvetica", 9)
        #cnv.setFillColorRGB(0,0,0)
        cnv.drawString(230, 700, "PERFORMANCE POR PDV (limite: 50)")

        cnv.drawImage(temp_file_gc_table, 20, 372,  width=550, height=320)

        cnv.drawImage(temp_file_gc_table2, 20, 124,  width=550, height=320)
        
        #ajustando a coluna de média
        #356
        mean_hight = 364
        for i in range(0, table_size):
            mean_hight -= 8
        cnv.drawImage(temp_file_gc_table3, 20, mean_hight,  width=550, height=320)


        cnv.save()


        ###################################### CSV
        full_df_merged_raymundo = df_executivo_filtered_b

        full_df_merged_raymundo = pd.merge(full_df_merged_raymundo, df_master_skus, how='left', on='SKU')
        full_df_merged_raymundo = full_df_merged_raymundo[['VisitID','Store Code', 'Store Name',
            'StartedAt', 'semana_iso',
            'Cod_Vend', 'Nome_Vend', 'e-mail_vend', 'Nome_GC',
            'e-mail_gc', 'Nome_GT', 'e-mail_gt',
            'SKU', 'value', 'barcode'
            ]]

        filtered_df = pd.merge(full_df_merged_raymundo, df_mix_obrigatorio, left_on=['SKU', 'barcode'], right_on=['SKU', 'SKU Barcode'])

        filtered_df.sort_values(by=['VisitID', 'Store Code', 'Store Name'], inplace=True)
        filtered_df['value'] = filtered_df.apply(lambda x: 'Presente' if x['value']==1 else 'Ruptura', axis=1)

        #att 9jan
        filtered_df = filtered_df.drop(['Cod_Vend','Nome_Vend', 'e-mail_vend', 'Nome_GC',
            'e-mail_gc', 'Nome_GT', 'e-mail_gt','barcode'], axis=1)

        filtered_df = filtered_df.rename({'VisitID':'ID_VISITA',
                                            'Store Code':'COD_LOJA',
                                            'Store Name':'NOME_LOJA',
                                            'StartedAt':'DATA_INICIO_PESQUISA',
                                            'value':'DISPONIBILIDADE',
                                            'SKU Barcode':'SKU_COD_BARRA'
                                            }, axis=1)
        #
        filtered_df['DATA_INICIO_PESQUISA'] = filtered_df['DATA_INICIO_PESQUISA'].dt.strftime('%Y-%m-%d %H:%M:%S')
        #
        filtered_df['link_vispera'] = filtered_df['ID_VISITA'].apply(lambda x: f'https://blueocean.vispera.co/projects/44/visits/{x}')

        caminho_arquivo_csv_must_hav = os.path.join(pasta_relatorios_vend, f'table_mix_obrigatorio_{executivo}_{data_atual}.xlsx')


        # Salva o DataFrame em um arquivo CSV
        filtered_df.to_excel(caminho_arquivo_csv_must_hav, index=False)
    return


#Salvando os dados para semanas posteriores
df_persistent.to_excel("Final_"+data_atual.replace("/", "_")+".xlsx", index=False)