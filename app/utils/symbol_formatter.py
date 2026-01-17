def format_symbol_string(script_output: str) -> str:
    lines = script_output.strip().splitlines()
    symbols = []
    for line in lines[1:]:
        parts = line.split(",")
        if parts:
            symbols.append(f"NSE:{parts[0].strip()}-EQ")
    return ",".join(symbols)
