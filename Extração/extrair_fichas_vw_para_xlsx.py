from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path
from typing import Iterable

import fitz  # PyMuPDF
import pandas as pd


DEFAULT_INPUT_DIR = Path(
    r"C:\Users\gabriel.vinicius\Documents\Vscode\Ficha Tecnicas - Veículos Pesados\VW"
)
DEFAULT_OUTPUT_XLSX = Path("vw_fichas_tecnicas.xlsx")


SECTION_PATTERNS = [
    (r"\bmotor\b", "Motor"),
    (r"\bcaixa de mudancas\b", "Transmissao"),
    (r"\btransmiss", "Transmissao"),
    (r"\bsistema de tracao\b", "Sistema de Tracao"),
    (r"\btracao\b", "Sistema de Tracao"),
    (r"\bembreagem\b", "Embreagem"),
    (r"\beixo dianteiro\b", "Eixo Dianteiro"),
    (r"\beixo traseiro motriz\b", "Eixo Traseiro Motriz"),
    (r"\bsuspensao\b", "Suspensao"),
    (r"\bchassi\b", "Chassi"),
    (r"\bchassis\b", "Chassi"),
    (r"\brodas e pneus\b", "Rodas e Pneus"),
    (r"\bfreios\b", "Freios"),
    (r"\bsistema eletrico\b", "Sistema Eletrico"),
    (r"\bvolumes de abastecimento\b", "Volumes de Abastecimento"),
    (r"\bdimens", "Dimensoes"),
    (r"\bpesos\b", "Pesos"),
    (r"\bdesempenho\b", "Desempenho"),
    (r"\bequipamentos\b", "Equipamentos"),
]

SECTION_ORDER = [
    "Geral",
    "Equipamentos",
    "Motor",
    "Transmissao",
    "Sistema de Tracao",
    "Embreagem",
    "Eixo Dianteiro",
    "Eixo Traseiro Motriz",
    "Suspensao",
    "Chassi",
    "Rodas e Pneus",
    "Freios",
    "Sistema Eletrico",
    "Volumes de Abastecimento",
    "Dimensoes",
    "Pesos",
    "Desempenho",
]


def normalize_text(text: str) -> str:
    text = (
        text.replace("\u2008", " ")
        .replace("\xa0", " ")
        .replace("\u2010", "-")
        .replace("\u2013", "-")
    )
    text = "".join(
        ch
        for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def clean_text(text: str) -> str:
    text = (
        text.replace("\u2008", " ")
        .replace("\xa0", " ")
        .replace("\u2010", "-")
        .replace("\u2013", "-")
    )
    return re.sub(r"\s+", " ", text).strip()


def should_skip_block(text: str) -> bool:
    n = normalize_text(text)
    if not n:
        return True
    if "dados tecnicos sujeitos a alteracoes sem aviso previo" in n:
        return True
    if "imagens meramente ilustrativas" in n:
        return True
    if n.startswith("edicao "):
        return True
    if n.startswith("star7 "):
        return True
    if re.fullmatch(r"[a-z]", n):
        return True
    if re.fullmatch(r"[a-z]( [a-z]){1,5}", n):
        return True
    return False


def infer_model_from_filename(pdf_path: Path) -> str:
    return pdf_path.stem.upper()


def detect_section(text: str) -> str | None:
    n = normalize_text(text)
    for pattern, section in SECTION_PATTERNS:
        if re.search(pattern, n):
            return section
    return None


def split_field_value(block_text: str) -> tuple[str, str]:
    lines = [clean_text(line) for line in block_text.splitlines() if clean_text(line)]
    if not lines:
        return "", ""
    if len(lines) >= 2:
        return lines[0], clean_text(" ".join(lines[1:]))
    line = lines[0]
    if ":" in line:
        left, right = line.split(":", 1)
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

    if not rows:
        rows.append(
            {
                "arquivo_pdf": pdf_path.name,
                "modelo": model,
                "pagina": None,
                "coluna_x": None,
                "linha_y": None,
                "secao": "Geral",
                "campo": "sem_texto_extraido",
                "valor": "",
                "bloco_texto": "",
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
    print("Aba 'tabela_por_secao' = 1 coluna por secao")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extrai dados dos PDFs VW e salva em XLSX."
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
