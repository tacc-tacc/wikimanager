from lxml import etree
import sqlite3
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
import unicodedata
from pathlib import Path
from psutil import disk_partitions

import tempfile
import os
import shutil
import subprocess
import sys
import re

from typing import TYPE_CHECKING, List, Optional, Set

if sys.version_info < (3, 10):
    from importlib_resources import files
else:
    from importlib.resources import files

from typing import (
    TYPE_CHECKING,
    Dict,
    Generator,
    List,
    Optional,
    Set,
    Tuple,
    TypedDict,
    Union,
    DefaultDict
)

BASE_DIR = "parsewiki/"

class Namespace:
    __slots__ = (
        "aliases",
        "canonicalName",
        "defaultContentModel",
        "hasGenderDistinction",
        "id",
        "isCapitalized",
        "isContent",
        "isIncludable",
        "isMovable",
        "isSubject",
        "isTalk",
        "name",
        "subject",
        "talk",
    )

    def __init__(
        self,
        aliases: Optional[List[str]] = None,
        canonicalName="",
        defaultContentModel="wikitext",
        hasGenderDistinction=True,
        id: Optional[int] = None,
        isCapitalized=False,
        isContent=False,
        isIncludable=False,
        isMovable=False,
        isSubject=False,
        isTalk=False,
        name="",
        subject: Optional["Namespace"] = None,
        talk: Optional["Namespace"] = None,
    ) -> None:
        assert name
        assert id is not None
        if aliases is None:
            aliases = []
        self.aliases: List[str] = aliases
        self.canonicalName = canonicalName
        self.defaultContentModel = defaultContentModel
        self.hasGenderDistinction = hasGenderDistinction
        self.id = id
        self.isCapitalized = isCapitalized
        self.isContent = isContent
        self.isIncludable = isIncludable
        self.isMovable = isMovable
        self.isSubject = isSubject
        self.isTalk = isTalk
        self.name = name
        self.subject = subject
        self.talk = talk

from pathlib import Path

NamespaceDataEntry = TypedDict(
    "NamespaceDataEntry",
    {
        "aliases": List[str],
        "content": bool,
        "id": int,
        "issubject": bool,
        "istalk": bool,
        "name": str,
    },
    total=True,  # fields are obligatory
)

EMPTY_NAMESPACEDATA: NamespaceDataEntry = {
    "id": -1,
    "name": "NAMESPACE_DATA_ERROR",
    "aliases": [],
    "content": False,
    "istalk": False,
    "issubject": False,
}


@dataclass
class Page:
    title: str
    namespace_id: int
    redirect_to: Optional[str] = None
    body: Optional[str] = None
    model: Optional[str] = None

from logger import logger

class DBManager:
    __slots__ = (
        "db_path",  # Database path
        "db_conn",  # Database connection
        "data_folder",
        "lang_code",
        "ns_data_path",
        "NAMESPACE_DATA",
        "LOCAL_NS_NAME_BY_ID",  # Local namespace names dictionary
        "NS_ID_BY_LOCAL_NAME",
        "namespaces",
        "namespace_ids",
        "namespace_names",
    )

    def __init__(self, lang_code="es"):
        self.db_path = Path(BASE_DIR+lang_code+"/db/wikidb.db")
        self.lang_code = lang_code  # dump file language code
        self.ns_data_path = Path(BASE_DIR+"data/"+lang_code+"/namespaces.json") #harcodeado

        with self.ns_data_path.open(encoding="utf-8") as f:
            self.NAMESPACE_DATA: Dict[str, NamespaceDataEntry] = json.load(f)
            self.LOCAL_NS_NAME_BY_ID: Dict[int, str] = {data["id"]: data["name"] for data in self.NAMESPACE_DATA.values()}
            self.NS_ID_BY_LOCAL_NAME: Dict[str, int] = {data["name"]: data["id"] for data in self.NAMESPACE_DATA.values()}

        self.namespaces: Dict[int, Namespace] = {}

        for ns_can_name, ns_data in self.NAMESPACE_DATA.items():
            self.namespaces[ns_data["id"]] = Namespace(
                id=ns_data["id"],
                name=ns_data["name"],
                isSubject=ns_data["issubject"],
                isContent=ns_data["content"],
                isTalk=ns_data["istalk"],
                aliases=ns_data["aliases"],
                canonicalName=ns_can_name,
            )
        
        for ns in self.namespaces.values():
            if ns.isContent and ns.id >= 0:
                ns.talk = self.namespaces[ns.id + 1]
            elif ns.isTalk:
                ns.subject = self.namespaces[ns.id - 1]

        self.namespace_ids = self.namespaces.keys()
        self.namespace_names = self.namespaces.values()

    @property
    def backup_db_path(self) -> Path:
        assert self.db_path
        return self.db_path.with_stem(self.db_path.stem + "_backup")
    
    def open_db(self) -> None:
        assert self.db_path

        if self.backup_db_path.exists():
            self.db_path.unlink(True)
            self.backup_db_path.rename(self.db_path)

        try:
            self.db_conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.db_conn.enable_load_extension(True)
            self.db_conn.load_extension('/usr/lib/sqlite3/pcre.so')

            #TEXT COLLATE BINARY o BLOB? creo que es lo mismo
            self.db_conn.executescript(
                """
            CREATE TABLE IF NOT EXISTS pages (
            title TEXT COLLATE BINARY,
            namespace_id INTEGER,
            redirect_to TEXT COLLATE BINARY,
            body TEXT COLLATE BINARY,
            model TEXT COLLATE BINARY,
            PRIMARY KEY(title, namespace_id));

            PRAGMA journal_mode = WAL;
            """
            )
        except sqlite3.OperationalError as e:
            logger.error("error de SQLITE: " + str(e))
            sys.exit()

    def decompress_dump_file(self, dump_path: str) -> subprocess.Popen:
        if dump_path.endswith(".bz2"):
            decompress_command = "lbzcat" if shutil.which("lbzcat") is not None else "bzcat"
            p = subprocess.Popen([decompress_command, dump_path], stdout=subprocess.PIPE)
            if p.stdout is not None:
                return p
            else:
                raise Exception(f"No stdout from command {decompress_command}")
        else:
            raise ValueError("Dump file extension is not .bz2")

    def parse_dump_xml(self, dump_path: str) -> None:
        with self.decompress_dump_file(dump_path) as p:
            namespace_str = "http://www.mediawiki.org/xml/export-0.10/"
            namespaces = {None: namespace_str}
            page_nums = 0
            for _, page_element in etree.iterparse(p.stdout, tag=f"{{{namespace_str}}}page"):
                title = page_element.findtext("title", "", namespaces)
                namespace_id = int(page_element.findtext("ns", "0", namespaces))
                if (namespace_id not in self.namespace_ids or title.endswith("/documentation") or "/testcases" in title):
                    page_element.clear(keep_tail=True)
                    continue

                text: Optional[str] = None
                redirect_to: Optional[str] = None
                model = page_element.findtext("revision/model", "", namespaces)
                if (redirect_element := page_element.find("redirect", namespaces=namespaces)) is not None:
                    redirect_to = redirect_element.get("title", "")
                    # redirect_to existing implies a redirection, but having a
                    # .get default to "" is a bit weird: redirect to empty string?
                    # But you can't use None either..?
                else:
                    if model not in {"wikitext", "Scribunto", "json"}:
                        # ignore css, javascript and sanitized-css pages
                        page_element.clear(keep_tail=True)
                        continue
                    text = page_element.findtext("revision/text", "", namespaces)

                self.add_page(title, namespace_id, body=text, redirect_to=redirect_to, model=model)
                page_element.clear(keep_tail=True)
                page_nums += 1
                if page_nums % 10000 == 0:
                    logger.info(f" {page_nums} raw pages collected")
                
        self.update_db()
        logger.info('agregado: '+str(self.saved_page_nums())+' entradas')           

    def backup_db(self) -> None:
        try:
            self.backup_db_path.unlink(True)
            self.db_conn.commit()
            backup_conn = sqlite3.connect(self.backup_db_path)
            with backup_conn:
                self.db_conn.backup(backup_conn)
            backup_conn.close()
        except sqlite3.OperationalError as e:
            logger.error("error de SQLITE: " + str(e))
            sys.exit()

    def update_db(self) -> None:
        try:
            self.db_conn.commit()
        except sqlite3.OperationalError as e:
            logger.error("error de SQLITE: " + str(e))
            sys.exit()

    def close_db(self) -> None:
        assert self.db_path
        self.update_db()
        self.db_conn.close()
        if self.db_path.parent.samefile(Path(tempfile.gettempdir())):
            for path in self.db_path.parent.glob(self.db_path.name + "*"):
                # also remove SQLite -wal and -shm file
                path.unlink(True)

    def build_sql_where_query(
        self,
        namespace_ids: Optional[List[int]] = None,
        include_redirects: bool = True,
        model: Optional[str] = None,
        search_pattern: Optional[str] = None,
        search_regex: Optional[str] = None,
        search_exclude: Optional[str] = None,
        regex_exclude: Optional[str] = None,
    ) -> Tuple[str, List[Union[str, int]]]:
        and_strs = []
        where_str = ""
        query_values = []
        if namespace_ids is not None:
            and_strs.append(f"namespace_id IN ({','.join('?' * len(namespace_ids))})")
            query_values.extend(namespace_ids)
        if not include_redirects:
            and_strs.append("redirect_to IS NULL")
        if search_pattern:
            if isinstance(search_pattern, List):
                assert len(search_pattern) > 0
                #en lugar de INSTR(body, ?), podría usar body LIKE ?, pero es insensible por defecto y yo quiero forzar a que sea SENSIBLE a todo
                if len(search_pattern) > 1:
                    and_strs.append('('+" OR ".join(["INSTR(body, ?)" for _ in search_pattern])+')')
                    query_values.extend([p for p in search_pattern])
                else:
                    and_strs.append("INSTR(body, ?)")
                    query_values.append(search_pattern[0])
            else:
                and_strs.append("INSTR(body, ?)")
                query_values.append(search_pattern)
        if search_regex:
            if isinstance(search_regex, List):
                assert len(search_regex) > 0
                if len(search_regex) > 1:
                    and_strs.append('('+" OR ".join(["body REGEXP ?" for _ in search_regex])+')')
                    query_values.extend([p for p in search_regex])
                else:
                    and_strs.append("body REGEXP ?")
                    query_values.append(search_regex[0])
            else:
                and_strs.append("body REGEXP ?")
                query_values.append(search_regex)
        if search_exclude:
            if isinstance(search_exclude, List):
                assert len(search_exclude) > 0
                if len(search_exclude) > 1:
                    and_strs.append('('+" AND ".join(["NOT INSTR(body, ?)" for _ in search_exclude])+')')
                    query_values.extend([p for p in search_exclude])
                else:
                    and_strs.append("NOT INSTR(body, ?)")
                    query_values.append(search_exclude[0])
            else:
                and_strs.append("NOT INSTR(body, ?)")
                query_values.append(search_exclude)
        if regex_exclude:
            if isinstance(regex_exclude, List):
                assert len(regex_exclude) > 0
                if len(regex_exclude) > 1:
                    and_strs.append('('+" AND ".join(["NOT body REGEXP ?" for _ in regex_exclude])+')')
                    query_values.extend([p for p in regex_exclude])
                else:
                    and_strs.append("NOT body REGEXP ?")
                    query_values.append(regex_exclude[0])
            else:
                and_strs.append("NOT body REGEXP ?")
                query_values.append(regex_exclude)
        if model is not None:
            and_strs.append("model = ?")
            query_values.append(model)

        if len(and_strs) > 0:
            where_str = "WHERE " + " AND ".join(and_strs)

        return where_str, tuple(query_values)

    def saved_page_nums(
        self,
        namespace_ids: Optional[List[int]] = None,
        include_redirects: bool = True,
        model: Optional[str] = None,
        search_pattern: Optional[str] = None,
        search_regex: Optional[str] = None,
        search_exclude: Optional[str] = None,
        regex_exclude: Optional[str] = None,
    ) -> int:
        query_str = "SELECT count(*) FROM pages"
        where_str, query_values = self.build_sql_where_query(namespace_ids, include_redirects, model, search_pattern, search_regex, search_exclude, regex_exclude)
        query_str += where_str

        try:
            for result in self.db_conn.execute(query_str, query_values):
                return result[0]
        except sqlite3.OperationalError as e:
            logger.error("error de SQLITE: " + str(e))
            sys.exit()

        return 0  # Mainly to satisfy the type checker


    def add_page(
        self,
        title: str,
        namespace_id: Optional[int],
        body: Optional[str] = None,
        redirect_to: Optional[str] = None,
        model: Optional[str] = "wikitext",
    ) -> None:
        """Collects information about the page and save page text to a SQLite database file."""
        if model is None:
            model = "wikitext"
        if namespace_id:
            ns_prefix = self.LOCAL_NS_NAME_BY_ID.get(namespace_id, "") + ":"
        else:
            ns_prefix = ""
        if namespace_id != 0 and not title.startswith(ns_prefix):
            title = ns_prefix + title

        if title.startswith("Main:"):
            title = title[5:]

        if (namespace_id == self.NAMESPACE_DATA.get("Template", {"id": None}).get("id") and redirect_to is None):
            body = self._template_to_body(title, body)

        try:
            self.db_conn.execute(
                """INSERT INTO pages (title, namespace_id, body, redirect_to, model) 
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(title, namespace_id) DO UPDATE SET body=excluded.body, redirect_to=excluded.redirect_to, model=excluded.model""",
                (title, namespace_id, body, redirect_to, model),
            )
        except sqlite3.OperationalError as e:
            logger.error("error de SQLITE: " + str(e))
            sys.exit()

    @lru_cache(maxsize=1000)
    def get_page(self, title: str, namespace_id: Optional[int] = None, no_redirect: bool = False,) -> Optional[Page]:
        # " " in Lua Module name is replaced by "_" in Wiktionary Lua code when call `require`
        title = title.replace("_", " ")
        if title.startswith("Main:"):
            title = title[5:]
        if len(title) == 0:
            return None

        upper_case_title = title  # the first letter is upper case
        if namespace_id is not None and namespace_id != 0:
            local_ns_name = self.LOCAL_NS_NAME_BY_ID[namespace_id]
            ns_prefix = local_ns_name + ":"
            if namespace_id in {self.NAMESPACE_DATA[ns]["id"] for ns in ["Template", "Module"]}:
                # Chinese Wiktionary and English Wikipedia capitalize the first
                # letter of template/module page titles but use lower case in
                # Wikitext and Lua code
                if title.startswith(ns_prefix):
                    template_name = title[len(ns_prefix) :]
                    upper_case_title = (ns_prefix + template_name[0].upper() + template_name[1:])
                else:
                    upper_case_title = ns_prefix + title[0].upper() + title[1:]
                    title = ns_prefix + title
            elif not title.startswith(ns_prefix):
                # Add namespace prefix
                title = ns_prefix + title

        query_str = """SELECT title, namespace_id, redirect_to, body, model FROM pages WHERE title = ?"""
        query_values = [title]
        if namespace_id is not None:
            query_str += " AND namespace_id = ?"
            query_values.append(namespace_id)
        if no_redirect:
            query_str += " AND redirect_to IS NULL"
        if upper_case_title != title:
            query_str = query_str + " UNION ALL " + query_str
            query_values = query_values + [upper_case_title] + query_values[1:]

        query_str += " LIMIT 1"
        try:
            for result in self.db_conn.execute(query_str, tuple(query_values)):
                return Page(
                    title=result[0],
                    namespace_id=result[1],
                    redirect_to=result[2],
                    body=result[3],
                    model=result[4],
                )
        except sqlite3.ProgrammingError as e:
            logger.error(f"{' '.join(e.args)} Current database file path: {self.db_path}")
            sys.exit()
        return None

    def page_exists(self, title: str) -> bool:
        return self.get_page(title) is not None

    def get_all_pages(
        self,
        namespace_ids: Optional[List[int]] = None,
        include_redirects: bool = True,
        model: Optional[str] = None,
        search_pattern: Optional[str] = None,
        search_regex: Optional[str] = None,
        search_exclude: Optional[str] = None,
        regex_exclude: Optional[str] = None,
    ) -> Generator[Page, None, None]:
        if not search_pattern:
            search_pattern = ' '

        query_str = """SELECT title, namespace_id, redirect_to, body, model FROM pages """
        where_str, query_values = self.build_sql_where_query(namespace_ids, include_redirects, model, search_pattern, search_regex, search_exclude, regex_exclude)
        query_str += where_str  
        
        # + " ORDER BY title ASC"
        # Seems that ORDER BY title doesn't actually matter in this case;
        # for other orderings it does, title just happens to be default
        # print("Getting all pages for query:"
        #       f"{query_str=!r}, {placeholders=!r}")

        for result in self.db_conn.execute(query_str, query_values):
            yield Page(
                title=result[0],
                namespace_id=result[1],
                redirect_to=result[2],
                body=result[3],
                model=result[4],
            )

    def get_page_resolve_redirect(self, title: str, namespace_id: Optional[int] = None) -> Optional[Page]:
        page = self.get_page(title, namespace_id)
        if page is None:
            return None
        if page.redirect_to is not None:
            return self.get_page(page.redirect_to, namespace_id, True)
        return page

    def _template_to_body(self, title: str, text: Optional[str]) -> str:
        """Extracts the portion to be transcluded from a template body."""
        assert isinstance(title, str)
        assert isinstance(text, str), (
            f"{text=!r} was passed " "into _template_to_body"
        )
        # Remove all comments
        text = re.sub(r"(?s)<!--.*?-->", "", text)
        # Remove all text inside <noinclude> ... </noinclude>
        text = re.sub(r"(?is)<noinclude\s*>.*?</noinclude\s*>", "", text)
        # Handle <noinclude> without matching </noinclude> by removing the
        # rest of the file.  <noinclude/> is handled specially elsewhere, as
        # it appears to be used as a kludge to prevent normal interpretation
        # of e.g. [[ ... ]] by placing it between the brackets.
        text = re.sub(r"(?is)<noinclude\s*>.*", "", text)
        # Apparently unclosed <!-- at the end of a template body is ignored
        text = re.sub(r"(?s)<!--.*", "", text)
        # <onlyinclude> tags, if present, include the only text that will be
        # transcluded.  All other text is ignored.
        onlys = list(
            re.finditer(
                r"(?is)<onlyinclude\s*>(.*?)"
                r"</onlyinclude\s*>|"
                r"<onlyinclude\s*/>",
                text,
            )
        )
        if onlys:
            text = "".join(m.group(1) or "" for m in onlys)
        # Remove <includeonly>.  They mark text that is not visible on the page
        # itself but is included in transclusion.  Also text outside these tags
        # is included in transclusion.
        text = re.sub(r"(?is)<\s*(/\s*)?includeonly\s*(/\s*)?>", "", text)
        return text
