const XLSX = require('xlsx');
const fs = require('fs');

const file = fs.readFileSync('/Users/alexandre/Documents/dev/gsus-bi/GSUS_ACEITE_2023_2026.csv');
const wb = XLSX.read(file, { type: 'buffer', cellDates: true });
const ws = wb.Sheets[wb.SheetNames[0]];
const data = XLSX.utils.sheet_to_json(ws, { defval: '', raw: true });

const targetEAS = [
    "HOSPITAL REGIONAL DO SUDOESTE",
    "HOSPITAL MUNICIPAL DE CASCAVEL",
    "HOSPITAL REGIONAL DE TOLEDO HRT",
    "HOSPITAL UNIVERSITARIO OESTE DO PARANA",
    "HOESP",
    "HOSPITAL BENEFICENTE ASSISTEGUAIRA",
    "ASSOCIACAO HOSPITALAR BENEFICENTE MOACIR",
    "HOSPITAL DR AURELIO",
    "HOSPITAL DE ENSINO SAO LUCAS",
    "HOSPITAL ITAMED",
    "HOSPITAL DO CANCER DE CASCAVEL UOPECCAN",
    "HOSPITAL MUNICIPAL PADRE GERMANO LAUCK",
    "CEONC",
    "DEUS MENINO - CEONC",
    "INSTITUTO SAO RAFAEL",
    "HOSPITAL SAO FRANCISCO",
    "POLICLINICA PATO BRANCO",
    "HOSPITAL MUNICIPAL PREFEITO QUINTO ABRAO"
];

const counts = {};
targetEAS.forEach(eas => counts[eas] = 0);

data.forEach(row => {
    const exec = String(row["EAS Executante"] || "").trim();
    if (exec) {
        const upperExec = exec.toUpperCase();
        if (counts.hasOwnProperty(upperExec)) {
            counts[upperExec]++;
        } else {
            const matched = targetEAS.find(eas => upperExec.includes(eas));
            if (matched) counts[matched]++;
        }
    }
});

console.log(counts);
