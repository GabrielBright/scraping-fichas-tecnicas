import pandas as pd
import os
import asyncio
from playwright.async_api import async_playwright
import logging
from tqdm import tqdm
import unicodedata
import re

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Arquivos
ARQUIVO_EXCEL_LINKS = "Links Mercedes Caminhoes Informacoes.xlsx"
ARQUIVO_EXCEL_DADOS = "dados_mercedes_caminhoes.xlsx"
ARQUIVO_CHECKPOINT = "checkpoint_mercedes.pkl"

# Função para normalizar texto (remove acentos, converte para minúsculas, remove espaços extras)
def normalizar_texto(texto):
    # Remove acentos
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    # Converte para minúsculas e remove espaços extras
    texto = re.sub(r'\s+', ' ', texto.lower().strip())
    # Substituições específicas
    texto = texto.replace("max.", "maxima").replace("n°", "numero").replace("/", " ")
    return texto

async def carregar_links():
    if not os.path.exists(ARQUIVO_EXCEL_LINKS):
        logging.error(f"Arquivo {ARQUIVO_EXCEL_LINKS} não encontrado.")
        return []
    try:
        df = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_LINKS, usecols=["Link"])
        links = df['Link'].dropna().unique().tolist()
        logging.info(f"Carregados {len(links)} links únicos do arquivo Excel: {links}")
        return links
    except Exception as e:
        logging.error(f"Erro ao carregar {ARQUIVO_EXCEL_LINKS}: {e}")
        return []

async def extrair_elemento(pagina, seletor, default="N/A"):
    try:
        elemento = pagina.locator(seletor)
        if await elemento.count() > 0:
            texto = (await elemento.first.inner_text()).strip()
            if texto:
                return texto
        return default
    except Exception as e:
        logging.debug(f"Erro ao extrair elemento {seletor}: {e}")
        return default

async def extrair_com_multiplos_seletores(pagina, seletores, default="N/A", link=""):
    for seletor in seletores:
        valor = await extrair_elemento(pagina, seletor)
        if valor != "N/A":
            logging.debug(f"Valor extraído com sucesso de '{seletor}' para {link}: {valor}")
            return valor
        else:
            logging.debug(f"Valor não encontrado em '{seletor}' para {link}")
    logging.debug(f"Nenhum seletor funcionou para {link}, retornando {default}")
    return default

async def extracaoDados(contexto, link, semaphore, retries=3):
    async with semaphore:
        pagina = await contexto.new_page()
        logging.info(f"Acessando {link}")
        for attempt in range(retries):
            try:
                response = await pagina.goto(link, timeout=80000)
                if response and response.status != 200:
                    logging.warning(f"Status {response.status} em {link}. Possível bloqueio.")
                    return None
                await pagina.wait_for_load_state('networkidle', timeout=80000)

                # Seletores para o modelo
                modelo_seletores = [
                    '.card-title',
                    'h1',
                    '.vehicle-title'
                ]

                # Extrair modelo
                modelo = await extrair_com_multiplos_seletores(pagina, modelo_seletores, link=link)

                # Extrair todos os itens da lista de especificações
                especificacoes = {}
                itens = await pagina.locator('//*[@id="especificacoes"]/div/ul/li').all()
                for item in itens:
                    texto = await item.inner_text()
                    if ":" in texto:
                        rotulo, valor = [parte.strip() for parte in texto.split(":", 1)]
                        rotulo_normalizado = normalizar_texto(rotulo)
                        if rotulo_normalizado in especificacoes:
                            # Se o rótulo já existe (e.g., múltiplas "Suspensão"), concatenar os valores
                            especificacoes[rotulo_normalizado] += " | " + valor
                        else:
                            especificacoes[rotulo_normalizado] = valor
                    else:
                        logging.debug(f"Item sem formato esperado (sem ':') em {link}: {texto}")

                # Campos esperados e suas variações
                campos_esperados = {
                    "Motor": ["motor"],
                    "Cilindros": ["cilindros"],
                    "Potência máxima": ["potencia maxima", "potencia"],
                    "Torque máximo": ["torque maximo", "torque"],
                    "Câmbio": ["cambio", "caixa de cambio"],
                    "Velocidade Máxima": ["velocidade maxima", "velocidade"],
                    "Pneus": ["pneus", "roda pneus"],
                    "Tração": ["tracao"],
                    "Altura": ["altura"],
                    "Largura": ["largura"],
                    "Comprimento total": ["comprimento total", "comprimento encarroçado", "comprimento maximo", "comprimento"],
                    "Entre eixos": ["entre eixos", "entre-eixos"],
                    "Carga útil máxima": ["carga util maxima", "carga util"],
                    "Peso bruto total": ["peso bruto total", "peso bruto", "pbt"],
                    "Tanque": ["tanque", "tanques", "tanque de combustivel"],
                    "Transmissão": ["transmissao"],
                    "Tomada de força": ["tomada de forca"],
                    "Embreagem": ["embreagem"],
                    "Nº marchas | Relações primeira/última": ["numero marchas relacoes primeira ultima", "n marchas relacoes primeira ultima", "numero marchas", "marchas"],
                    "Chassi escada, parafusado e rebitado, sem emenda atrás da cabina • material": ["chassi escada parafusado e rebitado sem emenda atras da cabina material", "chassi"],
                    "Tipo": ["tipo"],
                    "Suspensão": ["suspensao", "suspensao da cabine", "suspensao traseira"],
                    "Freios": ["freios", "freios e sistemas de seguranca", "freio de estacionamento", "freio auxiliar"]
                }

                # Preencher os dados com base nos rótulos
                dados = {"Modelo": modelo, "Link": link}
                for campo, sinonimos in campos_esperados.items():
                    valor_encontrado = "N/A"
                    for rotulo, valor in especificacoes.items():
                        rotulo_normalizado = normalizar_texto(rotulo)
                        # Verificar se algum sinônimo corresponde ao rótulo (ou contém o sinônimo como substring)
                        for sinonimo in sinonimos:
                            if sinonimo in rotulo_normalizado or rotulo_normalizado in sinonimo:
                                valor_encontrado = valor
                                break
                        if valor_encontrado != "N/A":
                            break
                    dados[campo] = valor_encontrado

                return dados
            except Exception as e:
                if attempt < retries - 1:
                    logging.warning(f"Tentativa {attempt + 1} falhou para {link}: {str(e)}. Tentando novamente após 2s...")
                    await asyncio.sleep(2)
                else:
                    logging.error(f"Erro em {link} após {retries} tentativas: {str(e)}")
                    return None
            finally:
                await pagina.close()

async def processar_links(links, max_concurrent=3):
    dados_coletados = []
    semaphore = asyncio.Semaphore(max_concurrent)
    if os.path.exists(ARQUIVO_CHECKPOINT):
        with open(ARQUIVO_CHECKPOINT, 'rb') as f:
            dados_coletados = pd.read_pickle(f).to_dict('records')
            processed_links = {d["Link"] for d in dados_coletados}
            links = [link for link in links if link not in processed_links]
            logging.info(f"Checkpoint carregado: {len(dados_coletados)} links já processados, {len(links)} restantes.")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        contexto = await navegador.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        try:
            tarefas = [extracaoDados(contexto, link, semaphore) for link in links]
            for tarefa in tqdm(asyncio.as_completed(tarefas), total=len(links), desc="Processando links"):
                resultado = await tarefa
                if resultado:
                    dados_coletados.append(resultado)
                    if len(dados_coletados) % 5 == 0:
                        pd.DataFrame(dados_coletados).to_pickle(ARQUIVO_CHECKPOINT)
                        logging.info(f"Checkpoint salvo com {len(dados_coletados)} links.")
                await asyncio.sleep(0.5)
        except Exception as e:
            logging.error(f"Erro no processamento: {e}")
        finally:
            await contexto.close()
            await navegador.close()

    return dados_coletados

async def salvar_dados(dados_coletados):
    if not dados_coletados:
        logging.warning("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados_coletados)
    await asyncio.to_thread(df.to_excel, ARQUIVO_EXCEL_DADOS, index=False)
    logging.info(f"Dados salvos em '{ARQUIVO_EXCEL_DADOS}' ({len(df)} registros).")

async def main():
    links = await carregar_links()
    if not links:
        logging.error("Nenhum link para processar.")
        return
    dados_coletados = await processar_links(links)
    await salvar_dados(dados_coletados)

if __name__ == "__main__":
    asyncio.run(main())