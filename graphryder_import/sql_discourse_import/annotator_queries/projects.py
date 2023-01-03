def get(log, db_root, db_cursor, omitted_projects):
    annotator_projects_query = f'''
    SELECT 
    id, name, created_at, updated_at
    FROM  {db_root}discourse_annotator_projects
    '''

    annotator_projects = {}
    project_list = ''
    db_cursor.execute(annotator_projects_query)
    annotator_projects_data = db_cursor.fetchall()
    for project in annotator_projects_data:
        if not project[1] in omitted_projects:
            pid = project[0]
            annotator_projects[pid] = {
                'id': pid,
                'name': project[1],
                'created_at': project[2],
                'updated_at':project[3]
            }
            project_list += f' {project[1]},'

    log.info(f'    Got {len(list(annotator_projects.keys()))} projects.')
    return annotator_projects, project_list
