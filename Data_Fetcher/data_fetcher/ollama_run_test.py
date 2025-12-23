from xml.etree.ElementTree import tostring

from rich import print_json
from ollama_transformer import transform
from pathlib import Path
from colorama import Fore, Style
import time


def load_text(path: str) -> str:
    """Read a text file (HTML, OFX, CSV as raw string)."""
    p = Path(path)
    with p.open('r', encoding='utf-8') as f:
        data = f.read()
    if not data.strip():
        raise ValueError(f"{p} is empty")
    return data

def demo() -> None:
    samples = [
      # (load_text("samples_for_LLM/transactions.html"), "transactions.html")
       # (load_text("samples_for_LLM/Tx.xml"),               "Tx.xml"),
       (load_text("samples_for_LLM/Txs.csv"),              "Txs.csv")
    ]

    print(Fore.CYAN + "=== Running local transformer tests via Ollama ===")

    for s, filename in samples:
        case_name = Path(filename).suffix[1:].upper() + " Case"   # ".xml" -> "XML Case"
        print(Fore.LIGHTGREEN_EX + f"\n──── {case_name} — {filename} ────" + Style.RESET_ALL)

        try:
            out = transform(s)
            print_json(data=out)  # pretty, colored JSON

            count = len(out["transactions"])
            print(Fore.LIGHTCYAN_EX+"Number of transaction data processed: ", count)
        except Exception as e:
            print(Fore.RED + f"ERROR in {filename}: {e}" + Style.RESET_ALL)

if __name__ == "__main__":
    start = time.time()  # mark script start
    demo()
    end = time.time()  # mark script end
    print(Fore.RED +"Total runtime:", round((end - start),2), "seconds")
