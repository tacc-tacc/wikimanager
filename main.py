from dbmanager import DBManager
from editor import Editor
import re

dbmanager = DBManager(lang_code='es')
dbmanager.open_db() #se abre es/db/wikidb.db (se crea si no existe)
#dbmanager.parse_dump_xml("wikimanager/es/db/eswiktionary-20240101-pages-meta-current.xml.bz2") la primera vez hay que abrir el bz2 para agregar las pags

#:*'''Grafías alternativas:''' --> mal, cambiar el ''' por la plantilla correspondiente
ERRORES_A_CORREGIR = [
    '{ES', 
    'ES}', 
    'ES|', 
    'TRANS', 
    'TRANSLIT', 
    'TAXO', 
    '{pronunc', #pronunciación
    '{estructura', 
    '<ref>Separac', #separación silábica
    '<ref>separac',
    '{transliter', #transliteración
    "'''pronun", #pronunciación
    "'''Pronun",
    "'''etimol", #etimología
    "'''Etimol",
    "'''ej", #ejemplo
    "'''Ej",
    "'''graf", #grafía
    "'''Graf",
    "'''sinoni", #sinónimo --> es :*'''sino
    "'''Sinoni",
    "'''sinóni", #sinónimo
    "'''Sinóni",
    "'''antoni", #antónimo
    "'''Antoni",
    "'''antóni", #antónimo
    "'''Antóni",
    "'''relac", #relacionado
    "'''Relac",
    "'''hip", #hiperónimo e hipónimo, hipocorístico
    "'''Hip",
    "'''hom", #homófono
    "'''Hom",
    "'''cohip", #cohipónimo
    "'''Cohip",
    "'''par", #parónimo
    "'''Par",
    "'''datac", #datación
    "'''Datac", 
    "'''uso", #uso
    "'''Uso",
    "'''ámbito", #ámbito
    "'''Ámbito",
    "'''ambito", #ámbito
    "'''Ambito",
    "'''anagrama", #anagrama
    "'''Anagrama",
    "'''transli", #transliteración
    "'''Transli",
    "'''variante", #variante
    "'''Variante", 
    "'''derivad", #derivado
    "'''Derivad", 
    "'''descend", #descendiente
    "'''Descend",
    "'''rima", #rima
    "'''Rima",
    "'''audio", #audio
    "'''Audio",
    ": '''en ", #;1: '''en Física y Química:''' --> cambiar por plantilla
    "{{escritura-ja",
]

def encontrar_paginas_con_errores():
    fp = open('wikimanager/es/mantenimiento/pag_con_errores.txt', 'w')
    i = 0
    for page in dbmanager.get_all_pages(namespace_ids=[0], include_redirects=True, search_pattern=ERRORES_A_CORREGIR, search_regex=None, search_exclude=None, regex_exclude=None):
        fp.write(page.title + '\n')
        i += 1
    print('hay que revisar: '+str(i)+' páginas :\'(')
    fp.close()

def corregir_pagina(titulo, cuerpo):
    editor = Editor()
    nuevo_cuerpo = editor.analizar(titulo, cuerpo)
    return nuevo_cuerpo

encontrar_paginas_con_errores()

dbmanager.close_db() #importante cerrar al final
