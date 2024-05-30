import os
from pdf2image import convert_from_path

def create_pngs():
    
    # Caminho da pasta contendo os arquivos PDF
    pasta_pdf = 'report_vendedores'
    
    # Lista todos os arquivos na pasta
    arquivos_pdf = [arquivo for arquivo in os.listdir(pasta_pdf) if arquivo.lower().endswith('.pdf')]
    
    # Loop através de cada arquivo PDF
    for pdf in arquivos_pdf:
        caminho_pdf = os.path.join(pasta_pdf, pdf)
    
        # Convertendo o PDF para imagens
        images = convert_from_path(caminho_pdf, 500, poppler_path='C:/Program Files/poppler-24.02.0/Library/bin')
    
        # Salvando as imagens no mesmo diretório com a extensão .jpeg
        for i, imagem in enumerate(images):
            nome_imagem = os.path.splitext(pdf)[0] + '.jpeg'
            caminho_imagem = os.path.join(pasta_pdf, nome_imagem)
            imagem.save(caminho_imagem, 'JPEG')

    return