from __future__ import annotations

from datetime import date


def fmt_money_brl(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


def fmt_pct_br(v: float) -> str:
    return f"{v:,.2f}%".replace(",", "_").replace(".", ",").replace("_", ".")


def fmt_int_br(v: int) -> str:
    return f"{int(v):,}".replace(",", ".")


def fmt_date_br(v: date | str) -> str:
    if isinstance(v, date):
        return v.strftime("%d/%m/%Y")
    try:
        return date.fromisoformat(str(v)).strftime("%d/%m/%Y")
    except Exception:
        return str(v)


def validate_html_ptbr(kind: str, html: str) -> None:
    if '<html lang="pt-BR">' not in html:
        raise ValueError("HTML sem lang='pt-BR'.")

    html_lower = html.lower()

    global_forbidden = [" nao ", "indisponivel", "lancador"]
    for token in global_forbidden:
        if token in html_lower:
            raise ValueError(f"HTML fora do padrão pt-BR (token proibido: {token!r}).")

    required_by_kind: dict[str, list[str]] = {
        "home": ["lançador diário", "operação", "somente leitura"],
        "status": ["status do ciclo diário", "mensagem", "voltar ao início"],
        "readonly": ["modo leitura", "somente leitura"],
        "painel": ["painel diário", "sessão relatório", "sessão boletim"],
    }
    required_tokens = required_by_kind.get(kind, [])
    for token in required_tokens:
        if token not in html_lower:
            raise ValueError(f"HTML pt-BR inválido ({kind}): faltou token obrigatório {token!r}.")
