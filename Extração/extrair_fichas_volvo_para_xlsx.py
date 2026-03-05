from __future__ import annotations
import argparse
import re
import unicodedata
from pathlib import Path
from typing import Iterable
import fitz  # PyMuPDF
import pandas as pd

DEFAULT_INPUT_DIR = Path(
    r"C:\Users\gabriel.vinicius\Documents\Vscode\Ficha Tecnicas - Veículos Pesados\Fichas Tecnicas VOLVO"
)
DEFAULT_OUTPUT_XLSX = Path("volvo_fichas_tecnicas.xlsx")

SECTION_PATTERNS = [
    (r"\bdimens", "Dimensoes"),
    (r"\bpesos\b", "Pesos"),
    (r"\bmotor\b", "Motor"),
    (r"\btransmiss", "Transmissao"),
    (r"\bfreios\b", "Freios"),
    (r"\beixos traseiros\b", "Eixos Traseiros"),
    (r"\beixo dianteiro\b", "Eixo Dianteiro"),
    (r"\bsuspensao traseira\b", "Suspensao Traseira"),
    (r"\bsuspensao dianteira\b", "Suspensao Dianteira"),
    (r"\bchassis\b", "Chassis"),
    (r"\bsistema eletrico\b", "Sistema Eletrico"),
    (r"\bembreagem\b", "Embreagem"),
    (r"\bcabines\b", "Cabines"),
]

SECTION_ORDER = [
    "Geral",
    "Dimensoes",
    "Pesos",
    "Motor",
    "Transmissao",
    "Freios",
    "Eixos Traseiros",
    "Eixo Dianteiro",
    "Suspensao Traseira",
    "Suspensao Dianteira",
    "Chassis",
    "Sistema Eletrico",
    "Embreagem",
    "Cabines",
]

def normalize_text(text: str) -> str:
    text = text.replace("\u2008", " ").replace("\xa0", " ").replace("\u2010", "-")
    text = "".join(
        ch
        for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text

def clean_text(text: str) -> str:
    text = text.replace("\u2008", " ").replace("\xa0", " ").replace("\u2010", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def should_skip_block(text: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return True
    if "desenvolvido pelo departamento de engenharia de vendas" in normalized:
        return True
    if "www.volvo.com.br" in normalized:
        return True
    return False

def infer_model_from_filename(pdf_path: Path) -> str:
    return pdf_path.stem.upper()

def detect_section(block_text: str) -> str | None:
    normalized = normalize_text(block_text)
    for pattern, section in SECTION_PATTERNS:
        if re.search(pattern, normalized):
            return section
    return None

def split_field_value(block_text: str) -> tuple[str, str]:
    lines = [clean_text(line) for line in block_text.splitlines() if clean_text(line)]
    if not lines:
        return "", ""
    if len(lines) >= 2:
        return lines[0], clean_text(" ".join(lines[1:]))

    line = lines[0]
    for sep in [":", " - "]:
        if sep in line:
            left, right = line.split(sep, 1)
            if clean_text(left) and clean_text(right):
                return clean_text(left), clean_text(right)
    return line, ""

def iter_pdf_blocks(pdf_path: Path) -> Iterable[tuple[int, float, float, str]]:
    with fitz.open(pdf_path) as doc:
        for page_idx, page in enumerate(doc, start=1):
            blocks = page.get_text("blocks")
            blocks_sorted = sorted(blocks, key=lambda b: (round(b[1]), b[0]))
            for block in blocks_sorted:
                x0, y0, _, _, raw_text, *_ = block
                text = raw_text.replace("\u2008", " ").replace("\xa0", " ")
                text = "\n".join(line.strip() for line in text.splitlines() if line.strip())
                if text:
                    yield page_idx, float(x0), float(y0), text

def extract_rows_from_pdf(pdf_path: Path) -> list[dict]:
    model = infer_model_from_filename(pdf_path)
    current_section = "Geral"
    rows: list[dict] = []

    for page, x0, y0, block_text in iter_pdf_blocks(pdf_path):
        block_one_line = clean_text(block_text.replace("\n", " "))
        if should_skip_block(block_one_line):
            continue

        section_guess = detect_section(block_one_line)
        if section_guess:
            current_section = section_guess

        field, value = split_field_value(block_text)

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
                "bloco_texto": block_one_line,
            }
        )

    return rows

def build_section_table(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return df_long

    grouped = (
        df_long.groupby(["arquivo_pdf", "modelo", "secao"], as_index=False)["bloco_texto"]
        .agg(lambda s: " | ".join(dict.fromkeys(v for v in s if str(v).strip())))
    )

    section_table = grouped.pivot_table(
        index=["arquivo_pdf", "modelo"],
        columns="secao",
        values="bloco_texto",
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
    pdf_files = sorted(input_dir.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"Nenhum PDF encontrado em: {input_dir}")

    all_rows: list[dict] = []
    for pdf in pdf_files:
        all_rows.extend(extract_rows_from_pdf(pdf))

    df_long = pd.DataFrame(all_rows)
    df_section = build_section_table(df_long)

    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        df_long.to_excel(writer, sheet_name="dados_detalhados", index=False)
        df_section.to_excel(writer, sheet_name="tabela_por_secao", index=False)

    print(f"OK: {len(pdf_files)} PDFs processados")
    print(f"OK: arquivo gerado em {output_xlsx.resolve()}")
    print("Aba 'dados_detalhados' = blocos extraidos com secao")
    print("Aba 'tabela_por_secao' = 1 coluna por secao (Motor, Pesos, etc.)")

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extrai dados dos PDFs Volvo e salva em XLSX."
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
        help="Arquivo XLSX de saida.",
    )
    args = parser.parse_args()

    run(args.input_dir, args.output)

if __name__ == "__main__":
    main()
