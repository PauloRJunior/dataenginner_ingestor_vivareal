from teste4 import process_imoveis, headersList, payload, get_json,start_process
import pandas as pd
from datetime import date
import os
import csv
import time

# Verifica se o arquivo status_processamento.csv existe
if os.path.exists('status_processamento.csv'):
    # Se o arquivo existir, carrega o DataFrame a partir do arquivo
    status_df = pd.read_csv('status_processamento.csv')
else:
    # Se o arquivo não existir, cria um DataFrame vazio com as colunas necessárias
    status_df = pd.DataFrame(columns=['Data', 'Cidade', 'Status', 'Tipo de Erro'])

# Obter a lista de cidades que precisam ser executadas com base no arquivo status_processamento.csv
cidades_para_executar = status_df[status_df['Status'] == 'Erro']['Cidade'].unique()

# Se o arquivo não existir ou houver cidades com erro, rodar todas as cidades
if not os.path.exists('status_processamento.csv') or len(cidades_para_executar) > 0:
    start_process(cidades_para_executar)
else:
    all_city = pd.read_csv('lista_capitais.csv')
    
    # all_city = all_city[all_city['Cidade'] == 'Macapá - AP']
    
    all_city = all_city.dropna()
    all_list = all_city['Cidade'].unique()
    start_process(all_list)
    print("Todas as cidades foram processadas com sucesso!")
    
#-----------SOLICITAR JSON PARA MAPEAR INFORMACOES
# url = "https://glue-api.vivareal.com/v2/listings?addressCity=Macap%C3%A1&addressLocationId=BR%3EAmapa%3ENULL%3EMacapa&addressNeighborhood=&addressState=Amap%C3%A1&addressCountry=Brasil&addressStreet=&addressZone=&addressPointLat=0.040522&addressPointLon=-51.056096&business=RENTAL&facets=amenities&unitTypes=&unitSubTypes=&unitTypesV3=&usageTypes=&listingType=USED&parentId=null&categoryPage=RESULT&includeFields=search(result(listings(listing(displayAddressType%2Camenities%2CusableAreas%2CconstructionStatus%2ClistingType%2Cdescription%2Ctitle%2CunitTypes%2CnonActivationReason%2CpropertyType%2CunitSubTypes%2Cid%2Cportal%2CparkingSpaces%2Caddress%2Csuites%2CpublicationType%2CexternalId%2Cbathrooms%2CusageTypes%2CtotalAreas%2CadvertiserId%2Cbedrooms%2CpricingInfos%2CshowPrice%2Cstatus%2CadvertiserContact%2CvideoTourLink%2CwhatsappNumber%2Cstamps)%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones%2Ctier)%2Cmedias%2CaccountLink%2Clink))%2CtotalCount)%2Cpage%2CseasonalCampaigns%2CfullUriFragments%2Cnearby(search(result(listings(listing(displayAddressType%2Camenities%2CusableAreas%2CconstructionStatus%2ClistingType%2Cdescription%2Ctitle%2CunitTypes%2CnonActivationReason%2CpropertyType%2CunitSubTypes%2Cid%2Cportal%2CparkingSpaces%2Caddress%2Csuites%2CpublicationType%2CexternalId%2Cbathrooms%2CusageTypes%2CtotalAreas%2CadvertiserId%2Cbedrooms%2CpricingInfos%2CshowPrice%2Cstatus%2CadvertiserContact%2CvideoTourLink%2CwhatsappNumber%2Cstamps)%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones%2Ctier)%2Cmedias%2CaccountLink%2Clink))%2CtotalCount))%2Cexpansion(search(result(listings(listing(displayAddressType%2Camenities%2CusableAreas%2CconstructionStatus%2ClistingType%2Cdescription%2Ctitle%2CunitTypes%2CnonActivationReason%2CpropertyType%2CunitSubTypes%2Cid%2Cportal%2CparkingSpaces%2Caddress%2Csuites%2CpublicationType%2CexternalId%2Cbathrooms%2CusageTypes%2CtotalAreas%2CadvertiserId%2Cbedrooms%2CpricingInfos%2CshowPrice%2Cstatus%2CadvertiserContact%2CvideoTourLink%2CwhatsappNumber%2Cstamps)%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones%2Ctier)%2Cmedias%2CaccountLink%2Clink))%2CtotalCount))%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones%2Ctier%2Cphones)%2Cowners(search(result(listings(listing(displayAddressType%2Camenities%2CusableAreas%2CconstructionStatus%2ClistingType%2Cdescription%2Ctitle%2CunitTypes%2CnonActivationReason%2CpropertyType%2CunitSubTypes%2Cid%2Cportal%2CparkingSpaces%2Caddress%2Csuites%2CpublicationType%2CexternalId%2Cbathrooms%2CusageTypes%2CtotalAreas%2CadvertiserId%2Cbedrooms%2CpricingInfos%2CshowPrice%2Cstatus%2CadvertiserContact%2CvideoTourLink%2CwhatsappNumber%2Cstamps)%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones%2Ctier)%2Cmedias%2CaccountLink%2Clink))%2CtotalCount))&size=100&from={}&q=&developmentsSize=5&__vt=B%2Ccontrol&levels=CITY&ref=&pointRadius=&isPOIQuery="

# json_data = get_json(url, i = 0 , headersList = headersList, payload = payload)
