from __future__ import annotations
import argparse
import re
import unicodedata
from pathlib import Path
from typing import Iterable
import fitz  # PyMuPDF
import pandas as pd

DEFAULT_INPUT_DIR = Path(r"C:\Users\gabriel.vinicius\Documents\Vscode\Ficha Tecnicas - Veículos Pesados\Fichas Tecnicas FORD")
DEFAULT_OUTPUT_XLSX = Path("ford_fichas_tecnicas.xlsx")

KNOWN_SECTIONS = {
    "motor",
    "transmissao (caixa de mudancas)",
    "embreagem",
    "eixo traseiro motriz",
    "suspensao",
    "sistema eletrico",
    "rodas e pneus",
    "freios",
    "direcao",
    "desempenho do veiculo (calculo teorico)",
    "pesos (kgf)",
    "longarina",
    "volumes de abastecimento (l)",
    "chassi/dimensoes (mm)",
    "dimensoes (mm)",
    "cores disponiveis",
    "especificacoes tecnicas",
}

SECTION_NAME_MAP = {
    "motor": "Motor",
    "transmissao (caixa de mudancas)": "Transmissao",
    "embreagem": "Embreagem",
    "eixo traseiro motriz": "Eixo Traseiro Motriz",
    "suspensao": "Suspensao",
    "sistema eletrico": "Sistema Eletrico",
    "rodas e pneus": "Rodas e Pneus",
    "freios": "Freios",
    "direcao": "Direcao",
    "desempenho do veiculo (calculo teorico)": "Desempenho",
    "pesos (kgf)": "Pesos",
    "longarina": "Longarina",
    "volumes de abastecimento (l)": "Volumes de Abastecimento",
    "chassi/dimensoes (mm)": "Chassi/Dimensoes",
    "dimensoes (mm)": "Dimensoes",
    "cores disponiveis": "Cores Disponiveis",
    "especificacoes tecnicas": "Especificacoes Tecnicas",
    "chassi/dimensoes (mm) transporte de bebidas": "Chassi/Dimensoes",
    "pesos (kgf) transporte de bebidas": "Pesos",
    "suspensao transporte de bebidas": "Suspensao",
    "volumes de abastecimento (l) transporte de bebidas": "Volumes de Abastecimento",
    "geral": "Geral",
}

SECTION_ORDER = [
    "Geral",
    "Motor",
    "Transmissao",
    "Embreagem",
    "Eixo Traseiro Motriz",
    "Suspensao",
    "Sistema Eletrico",
    "Rodas e Pneus",
    "Freios",
    "Direcao",
    "Desempenho",
    "Pesos",
    "Longarina",
    "Volumes de Abastecimento",
    "Chassi/Dimensoes",
    "Dimensoes",
    "Cores Disponiveis",
    "Especificacoes Tecnicas",
]

def normalize_text(text: str) -> str:
    text = text.replace("\u2008", " ").replace("\xa0", " ")
    text = "".join(
        ch
        for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text

def clean_text(text: str) -> str:
    text = text.replace("\u2008", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def is_section_heading(line: str) -> bool:
    normalized = normalize_text(line)
    if normalized in {"abs / asr / ebd"}:
        return False
    if normalized in KNOWN_SECTIONS:
        return True

    letters = [c for c in line if c.isalpha()]
    if not letters:
        return False

    upper_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
    has_digits = any(c.isdigit() for c in line)
    if upper_ratio >= 0.75 and not has_digits and 3 <= len(line) <= 55:
        return True

    return False

def should_skip_line(line: str) -> bool:
    normalized = normalize_text(line)
    if not normalized:
        return True

    patterns = [
        r"^spec_",
        r"^untitled",
        r"^disk ford",
        r"^\d{1,2}/\d{1,2}/\d{2,4}",
        r"^\d{1,2}:\d{2}",
    ]
    return any(re.search(pat, normalized) for pat in patterns)

def infer_model_from_filename(pdf_path: Path) -> str:
    stem = pdf_path.stem
    stem = stem.replace("-especificacoes-tecnicas", "")
    return stem.upper()


def canonical_section_name(section: str) -> str:
    normalized = normalize_text(section)
    return SECTION_NAME_MAP.get(normalized, section.strip().title())

def iter_pdf_blocks(pdf_path: Path) -> Iterable[tuple[int, float, float, str]]:
    with fitz.open(pdf_path) as doc:
        for page_idx, page in enumerate(doc, start=1):
            blocks = page.get_text("blocks")
            # Leitura por colunas: coluna da esquerda inteira, depois da direita.
            blocks_sorted = sorted(blocks, key=lambda b: (round(b[0] / 80), b[1], b[0]))
            for block in blocks_sorted:
                x0, y0, _, _, raw_text, *_ = block
                # Preserva quebras de linha internas para separar campo/valor.
                text = raw_text.replace("\u2008", " ").replace("\xa0", " ")
                text = "\n".join(line.strip() for line in text.splitlines() if line.strip())
                if text:
                    yield page_idx, float(x0), float(y0), text

def extract_rows_from_pdf(pdf_path: Path) -> list[dict]:
    model = infer_model_from_filename(pdf_path)
    current_section = "GERAL"
    rows: list[dict] = []

    for page, x0, y0, block_text in iter_pdf_blocks(pdf_path):
        lines = [clean_text(line) for line in block_text.splitlines() if clean_text(line)]
        if not lines:
            continue

        first_line = lines[0]
        if should_skip_line(first_line):
            continue

        if len(lines) == 1 and is_section_heading(first_line):
            current_section = first_line
            continue

        if len(lines) >= 2:
            field = lines[0]
            value = clean_text(" ".join(lines[1:]))
        else:
            single = lines[0]
            if ":" in single and single.count(":") == 1 and single.index(":") < 60:
                field, value = [clean_text(part) for part in single.split(":", 1)]
            else:
                field = single
                value = ""

        rows.append(
            {
                "arquivo_pdf": pdf_path.name,
                "modelo": model,
                "pagina": page,
                "coluna_x": round(x0, 1),
                "linha_y": round(y0, 1),
                "secao": current_section,
                "campo": field,
                "valor": value,
                "bloco_texto": block_text,
            }
        )

    return rows

def build_wide_table(df_long: pd.DataFrame) -> pd.DataFrame:
    df = df_long.copy()
    if df.empty:
        return df

    df["valor_efetivo"] = df["valor"].where(df["valor"].str.len() > 0, df["bloco_texto"])
    df["chave"] = (
        df["secao"].map(normalize_text)
        + " | "
        + df["campo"].map(normalize_text)
    )

    consolidated = (
        df.groupby(["arquivo_pdf", "modelo", "chave"], as_index=False)["valor_efetivo"]
        .agg(lambda s: " | ".join(dict.fromkeys(v for v in s if str(v).strip())))
    )

    wide = consolidated.pivot_table(
        index=["arquivo_pdf", "modelo"],
        columns="chave",
        values="valor_efetivo",
        aggfunc="first",
    ).reset_index()

    wide.columns.name = None
    return wide

def build_section_table(df_long: pd.DataFrame) -> pd.DataFrame:
    df = df_long.copy()
    if df.empty:
        return df

    df["secao_canonica"] = df["secao"].map(canonical_section_name)
    df["campo"] = df["campo"].fillna("").astype(str).str.strip()
    df["valor"] = df["valor"].fillna("").astype(str).str.strip()

    df["item"] = df.apply(
        lambda r: f"{r['campo']}: {r['valor']}" if r["valor"] else r["campo"],
        axis=1,
    )
    df["item"] = df["item"].str.strip()

    grouped = (
        df.groupby(["arquivo_pdf", "modelo", "secao_canonica"], as_index=False)["item"]
        .agg(lambda s: " | ".join(dict.fromkeys(v for v in s if v)))
    )

    section_table = grouped.pivot_table(
        index=["arquivo_pdf", "modelo"],
        columns="secao_canonica",
        values="item",
        aggfunc="first",
    ).reset_index()

    section_table.columns.name = None

    existing = [c for c in SECTION_ORDER if c in section_table.columns]
    remaining = [
        c
        for c in section_table.columns
        if c not in {"arquivo_pdf", "modelo"} and c not in existing
    ]
    ordered_cols = ["arquivo_pdf", "modelo", *existing, *sorted(remaining)]
    return section_table[ordered_cols]

def run(input_dir: Path, output_xlsx: Path) -> None:
    pdf_files = sorted(input_dir.glob("*especificacoes-tecnicas*.pdf"))
    if not pdf_files:
        pdf_files = sorted(input_dir.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"Nenhum PDF encontrado em: {input_dir}")

    all_rows: list[dict] = []
    for pdf in pdf_files:
        all_rows.extend(extract_rows_from_pdf(pdf))

    df_long = pd.DataFrame(all_rows)
    df_wide = build_wide_table(df_long)
    df_by_section = build_section_table(df_long)

    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        df_long.to_excel(writer, sheet_name="dados_detalhados", index=False)
        df_by_section.to_excel(writer, sheet_name="tabela_por_secao", index=False)
        df_wide.to_excel(writer, sheet_name="dados_consolidados", index=False)

    print(f"OK: {len(pdf_files)} PDFs processados")
    print(f"OK: arquivo gerado em {output_xlsx.resolve()}")
    print("Aba 'dados_detalhados' = linha a linha dos blocos")
    print("Aba 'tabela_por_secao' = 1 coluna por secao (Motor, Freios, etc.)")
    print("Aba 'dados_consolidados' = formato pivotado (tabela ampla)")

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extrai dados dos PDFs de fichas técnicas Ford e salva em XLSX."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Pasta com os PDFs (.pdf).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_XLSX,
        help="Arquivo XLSX de saída.",
    )
    args = parser.parse_args()

    run(args.input_dir, args.output)

if __name__ == "__main__":
    main()