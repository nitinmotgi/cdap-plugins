## Cask Hydrator Transform Plugin Collections.

### CSVParser

CSVParser takes a input field and parses in to a CSV Record with CSV Parser. The CSVParser supports different CSV formats like DEFAULT, MYSQL, EXCEL, RFC4180 and TDF.

### Masker
The Masker masks string field. Mask generated are of same length as the input field value. A seed is used to randomly select the characters that are used for Masking. 

### Hasher
The Hasher uses hashing algorithms to encode values of a field. Currently hasher supports MD2, MD5, SHA1, SHA256, SHA384, SHA512 algorithms for hashing a field. It's mainly used for encoding sensitive data like credit card numbers, social security numbers, and PII fields.
