import pdfplumber, sys, json

# Get the PDF content from database via a test file written by TS
# Instead, let's find a Chase PDF from the stored documents
import subprocess
result = subprocess.run(['node_modules/.bin/tsx', '-e', '''
import { db, documentsTable } from './src/configs/database';
import { eq } from 'drizzle-orm';
import fs from 'fs';
(async () => {
  const docs = await db.select({ id: documentsTable.id, name: documentsTable.name, storageKey: documentsTable.storageKey })
    .from(documentsTable)
    .where(eq(documentsTable.leadId, 100659));
  for (const d of docs) {
    if (d.name?.includes("9531001")) {
      // Find the file
      const key = d.storageKey;
      console.log(JSON.stringify({name: d.name, storageKey: key}));
      break;
    }
  }
  process.exit(0);
})();
'''], capture_output=True, text=True)
print("Doc info:", result.stdout.strip())
info = json.loads(result.stdout.strip())
storage_key = info.get('storageKey', '')
print(f"Storage key: {storage_key}")

# Try to find the actual file
import os, glob
for root, dirs, files in os.walk('uploads'):
    for f in files:
        if '9531001' in f and f.endswith('.pdf'):
            pdf_path = os.path.join(root, f)
            print(f"\nFound PDF: {pdf_path}")
            pdf = pdfplumber.open(pdf_path)
            for pi, page in enumerate(pdf.pages[:3]):
                chars = page.chars
                # Find chars that look like amounts (near right edge)
                # Group by y-position (line)
                lines = {}
                for c in chars:
                    y = round(c['top'], 0)
                    if y not in lines:
                        lines[y] = []
                    lines[y].append(c)
                
                print(f"\n=== PAGE {pi+1} ===")
                for y in sorted(lines.keys()):
                    line_chars = sorted(lines[y], key=lambda c: c['x0'])
                    text = ''.join(c['text'] for c in line_chars)
                    if text.strip()[:5].replace('/','').isdigit() and '/' in text[:5]:
                        # Transaction line - show with positions
                        first_x = line_chars[0]['x0']
                        last_x = line_chars[-1]['x1']
                        # Find where amount starts (rightmost cluster)
                        xs = [c['x0'] for c in line_chars]
                        # Find gap > 20 pixels
                        gaps = []
                        for i in range(1, len(line_chars)):
                            gap = line_chars[i]['x0'] - line_chars[i-1]['x1']
                            if gap > 5:
                                gaps.append((i, gap))
                        if gaps:
                            last_gap_idx = gaps[-1][0]
                            desc_part = ''.join(c['text'] for c in line_chars[:last_gap_idx])
                            amt_part = ''.join(c['text'] for c in line_chars[last_gap_idx:])
                            print(f"  Y={y:>6.0f} | DESC=[{desc_part}] | AMT=[{amt_part}] | gap={gaps[-1][1]:.0f}px")
                        else:
                            print(f"  Y={y:>6.0f} | FULL=[{text}] | NO GAP")
            pdf.close()
            sys.exit(0)

print("No PDF found")
