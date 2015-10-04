## Cask Hydrator Transform Plugin Collections.

### CSVParser

CSVParser is a Cask Hydrator ETL Transform that will parse a field a CSV. 
It supports multiple CSV formats like DEFAULT, MYSQL, EXCEL, RFC4180 and TDF.

Following are the specification of formats:

#### DEFAULT
1. Delimiter(',')
2. Quote('"')
3. Record Separator("\r\n") and
4. Ignores Empty Lines.

#### MYSQL
1. Delimiter('\t')
2. No Quote
3. Record Separator('\n')
4. Don't Ignore Empty Lines
5. Supports following Escape character('\\')

#### EXCEL
1. Delimiter(',')
2. Quote('"')
3. Record Separator("\r\n")
4. Don't Ignore Empty Lines
5. Allow Missing Column Names

#### RFC4180
1. Delimiter(',')
2. Quote('"')
3. Record Separator("\r\n")
4. Don't Ignore Empty Lines

#### TDF
1. Delimiter('\t')
2. Quote('"')
3. Record Separator("\r\n")
4. Ignore Surrounding Spaces(true)

### Masker
The Masker masks string field. Mask generated are of same length as the input field value. A seed is used to randomly select the characters that are used for Masking. 
