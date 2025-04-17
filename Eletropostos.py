import pandas as pd
import asyncio
import logging
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
URL = "https://abve.org.br/abve-data/bi-eletropostos/"
ARQUIVO_EXCEL = "eletropostos_completo.xlsx"

async def rolar_tabela_powerbi(pagina):
    logging.info("Iniciando rolagem do container interno do Power BI...")

    try:
        containers = pagina.locator(".scrollable-cells-viewport")
        total_containers = await containers.count()
        logging.info(f"Total de containers '.scrollable-cells-viewport' encontrados: {total_containers}")

        # Exibe os primeiros textos para ajudar a identificar o correto
        for i in range(min(5, total_containers)):
            try:
                texto = await containers.nth(i).inner_text()
                logging.info(f"[Container {i}] Início do texto: {texto[:100]}")
            except:
                logging.info(f"[Container {i}] Não foi possível extrair texto.")

        # Seleciona o container que provavelmente é da tabela
        container = containers.nth(2) 

        last_height = await container.evaluate("el => el.scrollHeight")
        for _ in range(20):
            await container.evaluate("el => el.scrollTop += 300")
            await asyncio.sleep(1)
            new_height = await container.evaluate("el => el.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        logging.info("Rolagem concluída.")
    except Exception as e:
        logging.error(f"Erro ao rolar a tabela: {str(e)}")
        raise

async def extrair_todas_linhas(pagina):
    await rolar_tabela_powerbi(pagina)

    linhas = pagina.locator("div[role='row']")
    total = await linhas.count()
    logging.info(f"Total de linhas detectadas: {total}")

    registros = []
    for i in range(total):
        try:
            celulas = linhas.nth(i).locator("div[role='gridcell']")
            valores = [ (await celulas.nth(j).inner_text()).strip() for j in range(await celulas.count()) ]
            if valores:
                registros.append(valores)
        except Exception as e:
            logging.warning(f"Erro na linha {i+1}: {str(e)}")
    return registros

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
        page = await context.new_page()

        try:
            logging.info(f"Acessando {URL}")
            await page.goto(URL, timeout=300000)
            await page.wait_for_load_state('networkidle', timeout=300000)
            logging.info("Página carregada. Esperando o Power BI renderizar...")
            await asyncio.sleep(180)

            # Localiza o iframe e acessa o conteúdo
            await page.wait_for_selector("iframe[src*='app.powerbi.com']", timeout=120000)
            iframe_element = await page.query_selector("iframe[src*='app.powerbi.com']")
            if not iframe_element:
                raise Exception("Iframe do Power BI não encontrado.")
            target_page = await iframe_element.content_frame()
            if target_page is None:
                raise Exception("Não foi possível acessar o conteúdo do iframe.")

            logging.info("Contexto do iframe pronto. Tentando localizar tabela...")
            try:
                await target_page.locator("text=/ELETROPOSTOS POR MUNIC[ÍI]PIO/i").wait_for(timeout=30000)
                logging.info("Título da tabela localizado.")
            except:
                logging.warning("Título da tabela não encontrado diretamente, prosseguindo mesmo assim...")

            # Extrai os dados
            # Extrai os dados
            dados = await extrair_todas_linhas(target_page)

            if not dados:
                logging.warning("Nenhum dado foi extraído.")
                return

            logging.info(f"Total de registros coletados: {len(dados)}")

            dados_corrigidos = []
            for linha in dados:
                if linha and linha[0] == "Select Row":
                    linha = linha[1:]
                if len(linha) == 6:
                    dados_corrigidos.append(linha)
                else:
                    logging.warning(f"❌ Linha ignorada (colunas: {len(linha)}): {linha}")

            if not dados_corrigidos:
                raise Exception("Nenhuma linha válida com 6 colunas após limpeza.")

            colunas = ["Município", "Estado", "AC (Recarga Lenta)", "DC (Recarga Rápida)", "Total", "MarketShare"]
            df = pd.DataFrame(dados_corrigidos, columns=colunas)
            df.to_excel(ARQUIVO_EXCEL, index=False)
            logging.info(f"{len(df)} linhas salvas com sucesso em: {ARQUIVO_EXCEL}")
        except Exception as e:
            logging.error(f"Erro durante a execução do script: {str(e)}")
            try:
                html_content = await page.content()
                with open("debug_page_final.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
            except Exception as e:
                logging.error(f"Erro ao salvar HTML para debug: {str(e)}")
            raise
        finally:
            await context.close()
            await browser.close()
            logging.info("Navegador fechado com sucesso.")

if __name__ == "__main__":
    asyncio.run(main())
