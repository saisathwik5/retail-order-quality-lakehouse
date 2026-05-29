# Public Data Source Notes

This repo ships with deterministic retail-order fixtures so tests and demos run
quickly. For a public source, use either:

- UCI Online Retail dataset: <https://archive.ics.uci.edu/dataset/352/online+retail>
- NYC TLC Trip Record data: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

The current implementation is retail-shaped because the Gold outputs are easy to
explain in interviews: revenue by region and daily order volume. To load the UCI
dataset, stage it in DBFS, create a Bronze reader for CSV/XLSX-exported records,
and map invoice, stock, quantity, price, customer, country, and invoice date
fields into the Silver order contract.

