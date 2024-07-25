import re
import os
import json
from ..database import *


def format():
    for filename in os.listdir(
        os.path.expanduser(rf"~/olympus/docs/_build/html/pages/")
    ):
        if ".html" in filename:
            with open(
                os.path.expanduser(rf"~/olympus/docs/_build/html/pages/") + filename,
                "r+",
            ) as f:
                page = f.read()

                regex = r"(?P<string>\<code\sclass\=\"docutils literal notranslate\"\>\<span class\=\"pre\"\>(?P<function>[\w\.]+)\.__doc__\<\/span\>\<\/code\>)"

                if re.search(regex, page):
                    matches = [match.groupdict() for match in re.finditer(regex, page)]

                    for match in matches:
                        i = 0

                        while i != len(match["function"].split(".")) - 1:
                            attr = getattr(
                                globals()[match["function"].split(".")[i]],
                                match["function"].split(".")[i + 1],
                            )

                            i += 1

                        docstr_regex = re.compile(
                            r"^\s+(?P<name>.*):\n(?P<values>(?:(?:\s+-\s.*)|(?:\s+>+\s.*))*)",
                            re.MULTILINE,
                        )

                        docstr_dict = {
                            match.groupdict()["name"]: (
                                match.groupdict()["values"]
                                if "values" in match.groupdict().keys()
                                else {match.groupdict()["name"]: ""}
                            )
                            for match in re.finditer(docstr_regex, attr.__doc__)
                        }

                        parameters_regex = re.compile(
                            r"^.*\-\s(?P<var_name>\w*)\:(?P<datatype>[^=]*)=?(?P<default>[^\s.|]*)?\s\-\s(?P<description>(?s)[^\.$]*)\.$",
                            re.MULTILINE,
                        )
                        returns_regex = re.compile(
                            r"^.*\-\s(?P<datatype>.*)?\s\-\s(?P<description>(?s)[^\.$]*)\.$",
                            re.MULTILINE,
                        )
                        see_also_regex = re.compile(
                            r"^.*\-\s(?P<name>.*)$", re.MULTILINE
                        )
                        description_regex = re.compile(
                            r".*\-\s(?P<description>.*\.)", re.MULTILINE
                        )

                        docstr_dict["Parameters"] = [
                            match.groupdict()
                            for match in re.finditer(
                                parameters_regex, docstr_dict["Parameters"]
                            )
                        ]
                        docstr_dict["Returns"] = [
                            match.groupdict()
                            for match in re.finditer(
                                returns_regex, docstr_dict["Returns"]
                            )
                        ]
                        docstr_dict["See Also"] = [
                            match.groupdict()
                            for match in re.finditer(
                                see_also_regex, docstr_dict["See Also"]
                            )
                        ]
                        docstr_dict["Description"] = re.search(
                            description_regex, docstr_dict["Description"]
                        ).groupdict()["description"]

                        parameters_head = [
                            (
                                rf"""<em class="sig-param"><span class="n"><span class="pre">{parameter['var_name']}</span></span><span class="p"><span class="pre">:</span></span> <span class="n"><span class="pre">{parameter['datatype']}</span></span> <span class="o"><span class="pre">=</span></span> <span class="default_value"><span class="pre">{parameter['default']}</span></span></em>"""
                                if "default" in parameter.keys()
                                else rf"""<em class="sig-param"><span class="n"><span class="pre">{parameter['var_name']}</span></span><span class="p"><span class="pre">:</span></span> <span class="n"><span class="pre">{parameter['datatype']}</span></span></em>"""
                            )
                            for parameter in docstr_dict["Parameters"]
                        ]

                        returns_head = [
                            rf"""{return_item["datatype"]}"""
                            for return_item in docstr_dict["Returns"]
                        ]

                        parameters_desc = [
                            rf"""<dd class="field-odd"><dl class="simple"><dt><strong>{parameter_item['var_name']}</strong><span class="classifier">{parameter_item['datatype']}</span></dt><dd><p>{parameter_item['description']}</p></dl></dd>"""
                            for parameter_item in docstr_dict["Parameters"]
                        ]

                        if parameters_desc != []:
                            parameters_desc = (
                                [
                                    rf"""
                                            <dt class="field-odd">Parameters</dt>
                                            <dd class="field-odd"><dl class="simple">
                                            """
                                ]
                                + parameters_desc
                                + [
                                    rf"""
                                            </dl>
                                            </dd>
                                            """
                                ]
                            )

                        returns_desc = [
                            rf"""<dd class="field-odd"><dl class="simple"><dt><strong>{return_item['datatype']}</strong><span class="classifier">{return_item['description']}</span></dt></dl></dd>"""
                            for return_item in docstr_dict["Returns"]
                        ]

                        if returns_desc != []:
                            returns_desc = (
                                [
                                    rf"""
                                        <dt class="field-odd">Returns</dt>
                                        <dd class="field-odd"><dl class="simple">
                                        """
                                ]
                                + returns_desc
                                + [
                                    rf"""
                                            </dl>
                                            </dd>
                                            """
                                ]
                            )

                        see_also = [
                            rf"""<dt><a class="reference internal" href="#{see_also_item['name']}" title="See Also title"><code class="xref py py-obj docutils literal notranslate"><span class="pre">{see_also_item['name']}</span></code></a></dt><dd>"""
                            for see_also_item in docstr_dict["See Also"]
                        ]

                        if see_also != []:
                            see_also = (
                                [
                                    rf"""
                                       <div class="admonition seealso">
                                       <p class="admonition-title">See also</p>
                                       <dl class="simple"> 
                                       """
                                ]
                                + see_also
                                + [
                                    rf"""
                                           </dd>
                                           </dl>
                                           </div>
                                           """
                                ]
                            )

                        html_str = rf"""
                        <dl class="py method">
                            <dt id="{match['function']}">
                            <code class="sig-name descname">
                                <span class="pre">{match['function']}</span>
                            </code>
                             <span class="sig-paren">(</span>{'<span class="pre">,</span>'.join(parameters_head)}<span class="sig-paren">)</span> &#x2192; {'<span class="pre">,</span>'.join(returns_head)}
                            </dt>
                            <br>
                            <br>
                            <dl class="field-list simple">
                            <dt class="field-odd">Description</dt>
                            <dd class="field-odd"><dl class="simple"><p>{docstr_dict['Description']}</p></dl></dd>
                            {''.join(parameters_desc)}
                            {''.join(returns_desc)}
                            </dl>
                            {''.join(see_also)}                    
                            </dd>
                        </dl>
                        <br>
                        """

                        page = page.replace(match["string"], html_str)
                        f.seek(0)
                        f.write(page)
                        f.truncate()

                f.close()
