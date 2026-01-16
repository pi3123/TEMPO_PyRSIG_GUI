#!/bin/bash
# Quick dump of all Python files for AI

OUTPUT="codebase_dump.txt"

echo "Dumping codebase to $OUTPUT..."

# Clear output file
> "$OUTPUT"

# Add header
echo "==================================" >> "$OUTPUT"
echo "TEMPO Analyzer Codebase Dump" >> "$OUTPUT"
echo "Generated: $(date)" >> "$OUTPUT"
echo "==================================" >> "$OUTPUT"
echo "" >> "$OUTPUT"

# Find all Python files and dump them
find src/tempo_app -name "*.py" -type f | while read file; do
    echo "" >> "$OUTPUT"
    echo "========================================" >> "$OUTPUT"
    echo "FILE: $file" >> "$OUTPUT"
    echo "========================================" >> "$OUTPUT"
    cat "$file" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
done

# Add requirements
echo "" >> "$OUTPUT"
echo "========================================" >> "$OUTPUT"
echo "FILE: requirements.txt" >> "$OUTPUT"
echo "========================================" >> "$OUTPUT"
cat requirements.txt >> "$OUTPUT"

echo "Done! Output saved to $OUTPUT"
wc -l "$OUTPUT"
