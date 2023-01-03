def get(log, db_root, db_cursor):
    annotator_languages_query = f'''
    SELECT
    id, name, locale
    FROM {db_root}discourse_annotator_languages
    '''

    annotator_languages = {}
    language_list = ''
    db_cursor.execute(annotator_languages_query)
    annotator_languages_data = db_cursor.fetchall()
    for language in annotator_languages_data:
        lid = language[0]
        annotator_languages[lid] = {
            'id': lid,
            'name': language[1],
            'locale': language[2]
        }
        language_list += f' {language[1]},'

    log.info(f'    Got annotation languages:{language_list[:-1]}.') 
    return annotator_languages, language_list
