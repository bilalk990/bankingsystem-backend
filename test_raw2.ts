import pdfParse from "pdf-parse";
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

const buf = fs.readFileSync(targetPdf);
pdfParse(buf).then(data => {
  const lines = data.text.split("\n");
  for (const line of lines) {
    if (/^\d{2}\/\d{2}/.test(line.trim()) || /DEPOSIT|WITHDRAWAL|ELECTRONIC/i.test(line.trim())) {
      console.log(`RAW: [${line.trim()}]`);
    }
  }
});
