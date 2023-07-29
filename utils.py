# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 16:15:12 2023

@author: andra
"""
import csv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import json
import time
from datetime import date


headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "x-domain": "https://www.vivareal.com.br"
}
payload = ""


def get_json(url, i, headersList, payload):
    ret = requests.request("GET", url.format(i), data=payload, headers=headersList)
    soup = bs(ret.text, 'html.parser')
    return json.loads(soup.text)

def process_imoveis(url, headersList, payload):
    df = pd.DataFrame(
        columns=[
            'descricao',
            'endereco',
            'area',
            'quartos',
            'suites',
            'wc',
            'vagas',
            'valor',
            'condominio',
            'w_link',
            'tipo',
            'cep',
            'lat',
            'lon'
        ]
    )
    imoveis_inseridos = []
    comprimento_anterior = None
    imovel_id = 0
    json_data = get_json(url, imovel_id, headersList, payload)
    while imovel_id < 10000: #len(json_data['search']['result']['listings']) > 0:
        qtd = len(json_data['search']['result']['listings'])
        print(f'Qtd de imóveis: {qtd} | Total: {imovel_id}')
        
        if comprimento_anterior is not None and len(df) == comprimento_anterior:
            print('Tamanho maximo atingindo')
            pass
        novos_imoveis = 0   
        for i in range(qtd):
            try:
                descricao = json_data['search']['result']['listings'][i]['listing']['title']
            except:
                descricao = '-'
            
            try:
                endereco = json_data['search']['result']['listings'][i]['listing']['address']['street'] + ", " + json_data['search']['result']['listings'][i]['listing']['address']['streetNumber']
            except:
                endereco = '-'
            
            try:
                area = json_data['search']['result']['listings'][i]['listing']['totalAreas'][0]
            except:
                area = '-'
            
            try:
                quartos = json_data['search']['result']['listings'][i]['listing']['bedrooms'][0]
            except:
                quartos = '-'
            
            try:
                suites = json_data['search']['result']['listings'][i]['listing']['suites'][0]
            except:
                suites = '-'
            
            try:
                wc = json_data['search']['result']['listings'][i]['listing']['bathrooms'][0]
            except:
                wc = '-'
            
            try:
                vagas = json_data['search']['result']['listings'][i]['listing']['parkingSpaces'][0]
            except:
                vagas = '-'
            
            try:
                valor = json_data['search']['result']['listings'][i]['listing']['pricingInfos'][0]['price']
            except:
                valor = '-'
            
            try:
                condominio = json_data['search']['result']['listings'][i]['listing']['pricingInfos'][0]['monthlyCondoFee']
            except:
                condominio = '-'
            
            try:
                w_link = 'https://www.vivareal.com.br' + json_data['search']['result']['listings'][i]['link']['href']
            except:
                w_link = '-'
                
            try:
                tipo = json_data['search']['result']['listings'][i]['listing']['unitTypes'][0]
            except:
                tipo = '-'
            
            try:
                cep = json_data['search']['result']['listings'][i]['listing']['address']['zipCode']
            except:
                cep = '-'
            
            try:
                lat = json_data['search']['result']['listings'][i]['listing']['address']['point']['lat']
            except:
                lat = '-'
            
            try:
                lon = json_data['search']['result']['listings'][i]['listing']['address']['point']['lon']
            except:
                lon = '-'
            
            
            
            id_anuncio = json_data['search']['result']['listings'][i]['listing']['id']
            
            
            if id_anuncio in imoveis_inseridos:
                pass
            else:
                df.loc[df.shape[0]] = [
                    descricao,
                    endereco,
                    area,
                    quartos,
                    suites,
                    wc,
                    vagas,
                    valor,
                    condominio,
                    w_link,
                    tipo,
                    cep,
                    lat,
                    lon
                ]

            novos_imoveis += 1
            imoveis_inseridos.append(id_anuncio)
            
        if novos_imoveis == 0:
            print("Nenhum novo imóvel adicionado. Finalizando a busca.")
            break
        
        tamanho_df = len(df)
        
        imovel_id = imovel_id + qtd
        
        time.sleep(2)
        
        json_data = get_json(url, imovel_id, headersList, payload)
        
        print(f'Anuncios encontrados: {len(df)}')
        
    return df


def start_process(cidades_para_executar):
    lista_capital = pd.read_csv('lista_capitais.csv')
    lista_capital = lista_capital.dropna()
    lista_capital = lista_capital.sort_values(by = 'Cidade', ascending = True)
    
    # Define today no escopo global para utilizá-la em todo o código
    today = str(date.today()).replace('-', '')

    # Inicializa o DataFrame vazio para armazenar os status das cidades
    status_df = pd.DataFrame(columns=['Data', 'Cidade', 'Status', 'Tipo de Erro'])

    for index, row in lista_capital.iterrows():
        cidade = row['Cidade']
        url = row['url']

        if cidade in cidades_para_executar:
            print(f"Processando cidade: {cidade}")
            cidade_lower = cidade.lower()

            cidade_lower = cidade_lower.split('-')[0]

            file_name_csv = f'bases_new/vivareal_{cidade_lower}_{today}.csv'
            file_name_json = f'bases_new/vivareal_{cidade_lower}_{today}.json'
            file_name_parquet = f'bases_new/vivareal_{cidade_lower}_{today}.parquet'
            
            df = pd.DataFrame()  # Cria um DataFrame vazio
            try:
                json_data = get_json(url, 0, headersList, payload)

                # Verificar se a chave 'search' existe no JSON
                if 'search' not in json_data:
                    raise ValueError("JSON inválido. A chave 'search' não existe.")

                df = process_imoveis(url, headersList, payload)
                
                #teste para ver se funciona
                colunas_convert = ['area','quartos','wc','vagas','valor','condominio','cep','suites','lat','lon']
                df[colunas_convert] = df[colunas_convert].astype(str)
                #teste para ver se funciona
                
                # Usar a biblioteca csv para salvar o arquivo com o delimitador correto
                with open(file_name_csv, 'w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file , delimiter = ';')
                    writer.writerow(df.columns)  # Escreve os cabeçalhos
                    writer.writerows(df.values)
                    
                df.to_json(file_name_json, orient='records' , lines = True)
                table = pa.Table.from_pandas(df)
                pq.write_table(table, file_name_parquet)
                
                print(f"Processamento concluído para cidade: {cidade}")

                # Atualiza o status da cidade para 'Sucesso' no DataFrame status_df
                new_status = pd.DataFrame({'Data': [today], 'Cidade': [cidade], 'Status': ['Sucesso'], 'Tipo de Erro': ['']})
                status_df = pd.concat([status_df, new_status], ignore_index=True)

            except Exception as e:
                print(f"Erro ao processar cidade {cidade}: {e}")
                # Atualiza o status da cidade para 'Erro' e o tipo de erro no DataFrame status_df
                new_status = pd.DataFrame({'Data': [today], 'Cidade': [cidade], 'Status': ['Erro'], 'Tipo de Erro': [str(e)]})
                status_df = pd.concat([status_df, new_status], ignore_index=True)

                # Se ocorrer um erro, ainda assim salva o arquivo CSV com os dados coletados até o momento
                try:
                    with open(file_name_csv, 'w', newline='', encoding='utf-8') as file:
                        writer = csv.writer(file, delimiter = ';')
                        writer.writerow(df.columns)  # Escreve os cabeçalhos
                        writer.writerows(df.values)
                    df.to_json(file_name_json, orient='records' , lines = True)
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, file_name_parquet)
                    
                except Exception as e:
                    print(f"Erro ao salvar arquivo CSV para cidade {cidade}: {e}")

            print("=" * 60)

    # Salvar o status atualizado no arquivo status_processamento.csv
    status_df.to_csv('status_processamento.csv', index=False)
    print("-------------------Processo Finalizado-------------------")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    