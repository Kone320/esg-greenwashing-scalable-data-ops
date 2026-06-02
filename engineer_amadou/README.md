# Engineer Amadou — Ingestion + Audit + Nettoyage

## Tâche
- Ingestion CSV depuis bronze S3
- Audit qualité (nulls, distributions)
- Imputation yoy_scope1_change_pct
- Encodage CDP ordinal (A=8 → F=1)
- Encodage Yes/No → 1/0

## Fichiers
- 01_audit_qualite.ipynb
- 02_nettoyage.ipynb

## Livrable S3
s3a://saragan/silver/esg_clean_step1/
