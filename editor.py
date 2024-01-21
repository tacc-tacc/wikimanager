
import definiciones
import wikitextparser as wtp
from wikitextparser import ExternalLink, WikiLink, Template, Comment, Bold, Italic, ParserFunction, Parameter, Table, SectionTitle, WikiList, Tag
import re

def replace_invalid_substrings(self, s: str) -> str:
    s = s.replace("//", "__slashslash__")
    if ".." in s:
        s = s.replace(".", "__dot__")
    return s    
RX_NO_LETRA = re.compile(r'[^a-zA-Z]+')
RX_SEPARACION_SILABICA = re.compile(r'[s|S]eparaci.n sil.bica:')
RX_PREFIJO_PLANTILLA = re.compile(r'[p|P]lantilla:[ \t]*')

ITEMS_A_REEMPLAZAR = [
    [re.compile('[p|P]ronunciaci.n:'), 'pron-graf'],
    [re.compile('[e|E]timolog.a:'), 'etimología'],
    [re.compile('[e|E]jemplo:'), 'ejemplo'],
    [re.compile('[s|S]in.nimo:'), 'sinónimo'],
    [re.compile('[a|A]nt.nimo:'), 'antónimo'],
    [re.compile('[h|H]iper.nimo:'), 'hiperónimo'],
    [re.compile('[h|H]ip.nimo:'), 'hipónimo'],
    [re.compile('[h|H]ipocor.stico:'), 'hipocorístico'],
    [re.compile('[c|C]ohip.nimo:'), 'cohipónimo'],
    [re.compile('[p|P]ar.nimo:'), 'parónimo'],
    [re.compile('[u|U]so:'), 'uso'],
    [re.compile('[a|A|á|Á]mbito:'), 'ámbito'],
    [re.compile('[a|A]nagrama:'), 'anagrama'],
    [re.compile('[d|D]erivado:'), 'derivad'],
    [re.compile('[d|D]escendiente:'), 'descendiente'],
]

TEXTO_A_REEMPLAZAR = [
    [re.compile('[a|A]testiguado desde el siglo '), 'datación'],
    [re.compile('[v|V]ariante de '), 'variante'],
]

AGREGAR_PRON_GRAF = [
    re.compile('[g|G]raf.a alternativa'),
    re.compile('[t|T]ransliteraci.n'),
    re.compile('[g|G]raf.a alternativa'),
    re.compile('[r|R]ima'),
    re.compile('[a|A]udio'),
]

#;1: '''en Física y Química:''' --> cambiar por plantilla


ROOT, L2, L3, L4, L5, L6, ITALIC, BOLD, LIST, LIST_ITEM, TAG, LINK, URL, TEMPLATE, PARSER_FN, MAGIC_WORD, TABLE = range(17) 

subt_to_kind = {
    "==": L2,
    "===": L3,
    "====": L4,
    "=====": L5,
    "======": L6,
}

kind_to_subt: {
    L2: "==",
    L3: "===",
    L4: "====",
    L5: "=====",
    L6: "======",
}

HAVE_ARGS = (LINK, TEMPLATE, PARSER_FN, URL,)
MUST_CLOSE = (ITALIC, BOLD, TAG, LINK, URL, TEMPLATE, PARSER_FN, TABLE)

_nowiki_map = {
    # ";": "&semi;",
    # "&": "&amp;",
    "=": "&equals;",
    "<": "&lt;",
    ">": "&gt;",
    "*": "&ast;",
    "#": "&num;",
    ":": "&colon;",
    "!": "&excl;",
    "|": "&vert;",
    "[": "&lsqb;",
    "]": "&rsqb;",
    "{": "&lbrace;",
    "}": "&rbrace;",
    '"': "&quot;",
    "'": "&apos;",
    "_": "&#95;",  # wikitext __MAGIC_WORDS__
}


UNIFICAR_PLANTILLA, \
REACOMODAR_SECCIONES, \
INSERTAR_SECCIONES, \
INSERTAR_PLANTILLA, \
INSERTAR_CIERRE, \
SUSTITUIR_TEXTO_PLANTILLA, \
SUSTITUIR_ITEM_PLANTILLA, \
ELIMINAR_PLANTILLA, \
ELIMINAR_COMENTARIO, \
ELIMINAR_DOBLE_PUNTO, \
ELIMINAR_DOBLE_ENTER, \
ELIMINAR_TITULO, \
ELIMINAR_ENLACE, \
ELIMINAR_TAG, \
CORREGIR_ORTOGRAFIA, \
CORREGIR_JERARQUIA, \
NORMALIZAR_ENLACE, \
NORMALIZAR_TITULO, \
NORMALIZAR_PLANTILLA, \
NORMALIZAR_LISTA, \
NORMALIZACION_DE_TAG, \
= range(21)

lista_de_correcciones = [
    'unificación de múltiples plantillas repetidas',
    'reacomodamiento del orden de las secciones',
    'inserción de plantillas faltantes', #clear, plm
    'inserción de secciones faltantes', #títulos, subtítulos, tabla de traducción, etc.
    'inserción de sintaxis de cierre',
    'sustitución de texto por plantillas',
    'sustitución de items por plantillas',
    'eliminación de plantillas inadecuadas',
    'eliminación de comentarios',
    'eliminación del doble punto',
    'eliminación del doble salto de línea',
    'eliminación de títulos inadecuados' #ver los de pronunciación y escritura; ver los que ponen '''la palabra''' antes de las acepciones como en enwikt (ej: '''sum''')
    'eliminación de enlaces inadecuados o redundantes',
    'eliminación de tags inadecuados o redundantes',
    'correcciones ortográficas',
    'correcciones jerárquicas',
    'normalización de enlaces',
    'normalización de títulos', #coregir ortografia y reemplazar por plantilla
    'normalización de plantillas',
    'normalización de listas', #corregir numeración y formato
    'normalización de tags' #ej: <ref name=dlc /> por <ref name="dlc" />
]

correcciones = [
    False,
    False,
    False,
    False,
    False,
    False
]

class WikiNode:
    __slots__ = {
        "kind",
        "level",
        "param",
        "args",
        "children"
    }

    def __init__(self, kind, level=-1, param=None, args=[], children=[]) -> None:
        self.kind = kind
        self.level = level
        self.param = param
        self.args = args
        self.children = children

class Editor:

    def __init__(self):
        pass

    def analizar(self, titulo, cuerpo):
        parser = wtp.parse(cuerpo)
        parser_stack = []
        parser_stack.extend(parser.comments)
        parser_stack.extend(parser.external_links)
        parser_stack.extend(parser.parameters)
        parser_stack.extend(parser.parser_functions)
        parser_stack.extend(parser.templates)
        parser_stack.extend(parser.wikilinks)
        parser_stack.extend(parser.get_section_titles())
        parser_stack.extend(parser.get_bolds_and_italics())
        parser_stack.extend(parser.get_lists())
        parser_stack.extend(parser.get_tables())
        parser_stack.extend(parser.get_tags())
        parser_stack.sort(key=lambda a:a.span[0]) #los ordeno por el orden en el que aparecen las cosas

        stack1 = []

        inicio_previo = 0
        fin_previo = 0
        for el in parser_stack:
            inicio = el.span[0]
            fin = el.span[1]

            if inicio >= fin_previo:
                if inicio > fin_previo:
                    stack1.append(parser._lststr[0][fin_previo:inicio])
                stack1.append(el)


            if inicio_previo < inicio:
                inicio_previo = inicio
            if fin_previo < fin:
                fin_previo = fin
        
        if len(parser._lststr[0]) > fin_previo:
            stack1.append(parser._lststr[0][fin_previo:])

        desambiguaciones = []
        unicode = None


        #con el stack1, que tiene todos los elementos en orden, hago el stack 2 con las plantillas viejas convertidas
        # a las nuevas plantillas


        stack2 = []

        l = len(stack1)
        i = 0
        while i < l:
            s = stack1[i]
            if isinstance(s, Template):
                tit = re.sub(RX_PREFIJO_PLANTILLA, '', tit)
                if s.title != tit:
                    correcciones[NORMALIZAR_PLANTILLA] = True

                if tit == "desambiguación":
                    desambiguaciones.extend(s.parameters)
                    if not desambiguaciones:
                        stack2.append(WikiNode(TEMPLATE, 1, 'desambiguación'))
                    else:
                        correcciones[UNIFICAR_PLANTILLA] = True
                
                elif tit == "unicode":
                    if not unicode:
                        match = re.findall("uniact=[0-9]+\|", s.parameters)
                        if not match:
                            raise ValueError
                        unicode = chr(int(match[0][7:-1]))
                        stack2.append(WikiNode(TEMPLATE, 1, 'mostrar-unicode'))
                    else:
                        raise ValueError
                

                elif tit == "pronunciación":
                    pass

                elif "ES" in s.title:
                    cod = s.title.split('-')[0]
                    cod = re.sub(RX_NO_LETRA, '', cod).lower()
                    stack2.append(WikiNode(TEMPLATE, 1, 'lengua|cod'))
                    correcciones[NORMALIZAR_PLANTILLA] = True
                
                elif s.title == "estructura":
                    correcciones[ELIMINAR_PLANTILLA] = True

                elif "Carácter Oriental" in s.title:
                    correcciones[ELIMINAR_PLANTILLA] = True

                elif "TRANSLIT" in s.title:
                    correcciones[ELIMINAR_PLANTILLA] = True

                elif "TAXO" in s.title:
                    correcciones[ELIMINAR_PLANTILLA] = True

            elif isinstance(s, Tag):
                if s.title == "ref":
                    match = re.findall(RX_SEPARACION_SILABICA, stack2[i+1].text)
                    if match:
                        #agregar la información de separación silábica
                        i += 1

            elif isinstance(s, Comment):
                correcciones[ELIMINAR_COMENTARIO] = True
            
            elif isinstance(s, SectionTitle):
                # tengo que volver a analizar el template
                correcciones[CORREGIR_JERARQUIA] = True

            elif isinstance(s, str):
                pass

            else:
                stack2.append(s)


            i += 1

                

                

                    

    

