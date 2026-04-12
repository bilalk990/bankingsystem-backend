#!/usr/bin/env python3
import sys
import json
import os
import tempfile
import subprocess
import re
import pdfplumber


def extract_text_with_column_detection(page):
    chars = page.chars
    if not chars:
        return page.extract_text() or ""

    lines_by_y = {}
    for c in chars:
        y_key = round(c['top'], 1)
        matched = False
        for existing_y in list(lines_by_y.keys()):
            if abs(existing_y - y_key) < 3:
                lines_by_y[existing_y].append(c)
                matched = True
                break
        if not matched:
            lines_by_y[y_key] = [c]

    result_lines = []
    for y in sorted(lines_by_y.keys()):
        line_chars = sorted(lines_by_y[y], key=lambda c: c['x0'])
        if not line_chars:
            continue

        parts = []
        current_part = [line_chars[0]]
        for i in range(1, len(line_chars)):
            gap = line_chars[i]['x0'] - line_chars[i - 1]['x1']
            if gap > 8:
                parts.append(current_part)
                current_part = [line_chars[i]]
            else:
                current_part.append(line_chars[i])
        parts.append(current_part)

        if len(parts) >= 2:
            text_parts = []
            for part in parts:
                text_parts.append(''.join(c['text'] for c in part))
            result_lines.append('  '.join(text_parts))
        else:
            result_lines.append(''.join(c['text'] for c in line_chars))

    return '\n'.join(result_lines)


def extract_with_pdfplumber(pdf_path):
    pages_text = []
    with pdfplumber.open(pdf_path) as pdf:
        num_pages = len(pdf.pages)
        is_bank_stmt = False
        for page in pdf.pages[:2]:
            quick_text = page.extract_text() or ""
            if re.search(r'(?i)(checking\s+summary|statement\s+period|deposits?\s+and|withdrawals?|bank\s+statement)', quick_text):
                is_bank_stmt = True
                break

        for page in pdf.pages:
            if is_bank_stmt:
                text = extract_text_with_column_detection(page)
            else:
                text = page.extract_text() or ""

            tables = page.extract_tables()
            table_text = ""
            if tables:
                for table in tables:
                    for row in table:
                        cleaned = [str(cell).strip() if cell else "" for cell in row]
                        table_text += "  |  ".join(cleaned) + "\n"
            combined = text
            if table_text and table_text.strip() not in text:
                combined += "\n--- TABLE DATA ---\n" + table_text
            pages_text.append(combined)
    full_text = "\n\n".join(pages_text)
    return full_text.strip(), num_pages


def extract_with_ocr(pdf_path):
    tmp_out = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp_out.close()
    try:
        subprocess.run(
            ["ocrmypdf", "--skip-text", "--force-ocr", "-l", "eng",
             "--output-type", "pdf", pdf_path, tmp_out.name],
            capture_output=True, text=True, timeout=120
        )
        text, num_pages = extract_with_pdfplumber(tmp_out.name)
        return text, num_pages
    except Exception as e:
        return f"[OCR error: {str(e)}]", 0
    finally:
        try:
            os.unlink(tmp_out.name)
        except:
            pass


def is_garbled_text(text):
    cid_count = text.count("(cid:")
    if cid_count > 20:
        return True
    alpha_chars = sum(1 for c in text if c.isalpha())
    if alpha_chars > 0 and cid_count / max(alpha_chars, 1) > 0.3:
        return True
    return False


def process_single(pdf_path):
    try:
        text, num_pages = extract_with_pdfplumber(pdf_path)
        stripped = text.strip()
        is_scanned = len(stripped) < 50 and num_pages > 0
        if not is_scanned and num_pages > 0 and is_garbled_text(stripped):
            is_scanned = True
        method = "pdfplumber"
        if is_scanned:
            method = "ocrmypdf"
            text, num_pages = extract_with_ocr(pdf_path)
        return {
            "success": True,
            "text": text,
            "numPages": num_pages,
            "method": method,
            "charCount": len(text)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "text": "", "numPages": 0}


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: extract_pdf.py <path> [path2 ...]"}))
        sys.exit(1)

    if "--batch" in sys.argv:
        batch_input = json.loads(sys.stdin.read())
        results = {}
        for key, pdf_path in batch_input.items():
            results[key] = process_single(pdf_path)
        print(json.dumps(results))
    elif len(sys.argv) == 2:
        result = process_single(sys.argv[1])
        print(json.dumps(result))
    else:
        results = {}
        for pdf_path in sys.argv[1:]:
            key = os.path.basename(pdf_path).split("_", 1)[0]
            results[key] = process_single(pdf_path)
        print(json.dumps(results))


if __name__ == "__main__":
    main()
