import pdfplumber from "pdfplumber";
import fs from "fs";
import path from "path";

const uploadsDir = path.join(process.cwd(), "uploads");
const dirs = fs.readdirSync(uploadsDir).filter(d => fs.statSync(path.join(uploadsDir, d)).isDirectory());
let targetPdf = "";
for (const dir of dirs) {
  const files = fs.readdirSync(path.join(uploadsDir, dir));
  for (const f of files) {
    if (f.includes("9531001") && f.endsWith(".pdf")) {
      targetPdf = path.join(uploadsDir, dir, f);
      break;
    }
  }
  if (targetPdf) break;
}
if (!targetPdf) { console.log("No PDF found"); process.exit(1); }
console.log("PDF:", targetPdf);

(async () => {
  const pdf = await pdfplumber.open(targetPdf);
  for (let pi = 0; pi < Math.min(pdf.pages.length, 3); pi++) {
    const page = pdf.pages[pi];
    const text = await page.extract_text();
    console.log(`\n===== PAGE ${pi + 1} =====`);
    const lines = text.split("\n");
    for (const line of lines) {
      if (/^\d{2}\/\d{2}/.test(line) || /DEPOSIT|WITHDRAWAL|ELECTRONIC/i.test(line)) {
        console.log(`  RAW: [${line}]`);
      }
    }
  }
  await pdf.close();
})();
