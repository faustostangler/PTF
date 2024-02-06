import assets.functions as run

import os


# system
start = 1
batch = 120
bins = 20
bin_size = 50
chunksize = 10**6  # Adjust this value based on your available memory

dataset_folder = 'datasets'
dataset_path = os.curdir + '/' + dataset_folder
dataset_path = run.check_or_create_folder(dataset_path)

columns = {}
columns['acoes'] = ['companhia', 'trimestre', 'conta', 'descrição', 'valor', 'unidade', 'demo_det', 'demo_fin', 'url']
columns['rad'] = ['companhia', 'trimestre', 'conta', 'descrição', 'valor', 'unidade', 'demo_det', 'demo_fin', 'url']
columns['nsd'] = ['companhia', 'trimestre', 'versao', 'dri', 'dri2', 'dre', 'auditor', 'auditor_rt', 'cancelamento', 'protocolo', 'envio', 'url', 'nsd']
columns['companies'] = ['companhia', 'pregao', 'cvm', 'listagem', 'ticker', 'tickers', 'asin', 'cnpj', 'site', 'setor', 'subsetor', 'segmento', 'atividade', 'escriturador', 'stock_holders', 'url']
columns['tickers'] = ['companhia', 'ticker', 'pregao', 'listagem']
columns['cvm_filelist'] = ['filename', 'date']

conta_acoes = [
    ['0.01.01', 'Ações ON'], 
    ['0.01.02', 'Ações PN'], 
    ['0.02.01', 'Ações ON em Tesouraria'], 
    ['0.02.02', 'Ações PN em Tesouraria'], 
    ]

dfp = ['INFORMACOES TRIMESTRAIS', 'DEMONSTRACOES FINANCEIRAS PADRONIZADAS']

nsd = 0
url = {}
url['nsd_pre'] = f'https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento='
url['nsd_pos'] = f'&CodigoTipoInstituicao=1'
url['b3_search'] = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/search?language=pt-br' 
url['dados_abertos_cvm'] = f'https://dados.cvm.gov.br/dados/CIA_ABERTA/'

visited_subfolders = set()

abbreviations_dict = {
    "NM": "Cia. Novo Mercado",
    "N1": "Cia. Nível 1 de Governança Corporativa",
    "N2": "Cia. Nível 2 de Governança Corporativa",
    "MA": "Cia. Bovespa Mais",
    "M2": "Cia. Bovespa Mais Nível 2",
    "MB": "Cia. Balcão Org. Tradicional",
    "DR1": "BDR Nível 1",
    "DR2": "BDR Nível 2",
    "DR3": "BDR Nível 3",
    "DRE": "BDR de ETF",
    "DRN": "BDR Não Patrocinado"
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 5.1; rv:36.0) Gecko/20100101 Firefox/36.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/83.0.478.37",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/53.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134"
]
REFERERS = [
    'https://www.google.com/',
    'https://www.bing.com/',
    'https://www.yahoo.com/',
    'https://www.facebook.com/',
    'https://twitter.com/',
    'https://www.reddit.com/',
    'https://www.youtube.com/',
    'https://www.linkedin.com/',
    'https://www.instagram.com/',
    'https://www.pinterest.com/',
    'https://www.wikipedia.org/',
    'https://www.amazon.com/',
    'https://www.ebay.com/',
    'https://www.craigslist.org/',
    'https://www.github.com/',
    'https://stackoverflow.com/',
    'https://www.quora.com/',
    'https://news.ycombinator.com/',
    'https://www.netflix.com/',
    'https://www.twitch.tv/',
    'https://www.spotify.com/',
    'https://www.tumblr.com/',
    'https://www.medium.com/',
    'https://www.dropbox.com/',
    'https://www.paypal.com/'
]
LANGUAGES = ['en-US;q=1.0', 'es-ES;q=0.9', 'fr-FR;q=0.8', 'de-DE;q=0.7', 'it-IT;q=0.6', 'pt-BR;q=0.9', 'ja-JP;q=0.8', 'zh-CN;q=0.7', 'ko-KR;q=0.6', 'ru-RU;q=0.9', 'ar-SA;q=0.8', 'hi-IN;q=0.7', 'tr-TR;q=0.6', 'nl-NL;q=0.9', 'sv-SE;q=0.8', 'pl-PL;q=0.7', 'fi-FI;q=0.6', 'da-DK;q=0.9', 'no-NO;q=0.8', 'hu-HU;q=0.7', 'ro-RO;q=0.6', 'cs-CZ;q=0.9', 'el-GR;q=0.8', 'th-TH;q=0.7', 'id-ID;q=0.6']

# BROWSER
chromedriver_path = r"D:\Fausto Stangler\Documentos\Python\PTF\assets\chromedriver-win64\chromedriver.exe"

# NSD
nsd_safety_factor=1.8
max_gap = 50
nsd_words_to_remove = ['  EM LIQUIDACAO', ' EM LIQUIDACAO', ' EXTRAJUDICIAL', '  EM RECUPERACAO JUDICIAL', '  EM REC JUDICIAL', ' EM RECUPERACAO JUDICIAL', ' EM LIQUIDACAO EXTRAJUDICIAL']


# Google GCS
json_key_file = 'credentials\storage admin.json'
bucket_name = 'b3_bovespa_bucket'
