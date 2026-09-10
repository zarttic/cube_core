#!/usr/bin/env python3
"""Update section 5.3 of the test plan document with actual test cases."""
from __future__ import annotations

import shutil
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

W = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
NS = {'w': W}
ET.register_namespace('w', W)
PROJECT_ID = "LZ006-YW-02-04"

TEST_CASES = [
    (1, "齐套性检查", f"{PROJECT_ID}-QT", "齐套性检查"),
    (2, "格网定位功能", f"{PROJECT_ID}-GN-001", "功能测试"),
    (3, "格网覆盖功能", f"{PROJECT_ID}-GN-002", "功能测试"),
    (4, "格网拓扑查询功能", f"{PROJECT_ID}-GN-003", "功能测试"),
    (5, "时空编码生成与解析", f"{PROJECT_ID}-GN-004", "功能测试"),
    (6, "光学影像逻辑剖分", f"{PROJECT_ID}-GN-005", "功能测试"),
    (7, "光学影像实体剖分", f"{PROJECT_ID}-GN-006", "功能测试"),
    (8, "雷达数据逻辑剖分", f"{PROJECT_ID}-GN-007", "功能测试"),
    (9, "雷达数据实体剖分", f"{PROJECT_ID}-GN-008", "功能测试"),
    (10, "产品数据逻辑剖分", f"{PROJECT_ID}-GN-009", "功能测试"),
    (11, "产品数据实体剖分", f"{PROJECT_ID}-GN-010", "功能测试"),
    (12, "碳卫星数据剖分", f"{PROJECT_ID}-GN-011", "功能测试"),
    (13, "剖分任务管理", f"{PROJECT_ID}-GN-012", "功能测试"),
    (14, "剖分配置管理", f"{PROJECT_ID}-GN-013", "功能测试"),
    (15, "批次管理", f"{PROJECT_ID}-GN-014", "功能测试"),
    (16, "质检报告管理", f"{PROJECT_ID}-GN-015", "功能测试"),
    (17, "数据入库管理", f"{PROJECT_ID}-GN-016", "功能测试"),
    (18, "用户认证管理", f"{PROJECT_ID}-GN-017", "功能测试"),
    (19, "单景剖分速度", f"{PROJECT_ID}-XN-001", "性能测试"),
    (20, "质检速度", f"{PROJECT_ID}-XN-002", "性能测试"),
    (21, "格网编码SDK接口", f"{PROJECT_ID}-JK-001", "接口测试"),
    (22, "剖分任务API接口", f"{PROJECT_ID}-JK-002", "接口测试"),
    (23, "质检报告API接口", f"{PROJECT_ID}-JK-003", "接口测试"),
    (24, "配置管理API接口", f"{PROJECT_ID}-JK-004", "接口测试"),
    (25, "数据入库API接口", f"{PROJECT_ID}-JK-005", "接口测试"),
    (26, "认证鉴权API接口", f"{PROJECT_ID}-JK-006", "接口测试"),
]


def set_cell_text(cell, text):
    """Completely clear cell content and set single text value."""
    # Remove all existing children
    for child in list(cell):
        cell.remove(child)
    # Create single paragraph with single run with single text
    p = ET.SubElement(cell, f'{{{W}}}p')
    r = ET.SubElement(p, f'{{{W}}}r')
    t = ET.SubElement(r, f'{{{W}}}t')
    t.text = text
    t.set('{http://www.w3.org/XML/1998/namespace}space', 'preserve')


def fill_table_18(tbl):
    rows = tbl.findall(f'{{{W}}}tr')
    for i, (seq, name, code, category) in enumerate(TEST_CASES):
        if i + 1 < len(rows):
            tr = rows[i + 1]
        else:
            tr = ET.SubElement(tbl, f'{{{W}}}tr')
            for _ in range(3):
                ET.SubElement(tr, f'{{{W}}}tc')
        cells = tr.findall(f'{{{W}}}tc')
        if len(cells) >= 3:
            set_cell_text(cells[0], str(seq))
            set_cell_text(cells[1], f"{name}/{code}")
            set_cell_text(cells[2], category)
    while len(rows) > len(TEST_CASES) + 1:
        tbl.remove(rows[-1])
        rows = tbl.findall(f'{{{W}}}tr')


def fill_table_14(tbl):
    rows = tbl.findall(f'{{{W}}}tr')
    if len(rows) >= 2:
        cells = rows[1].findall(f'{{{W}}}tc')
        if len(cells) >= 3:
            set_cell_text(cells[2],
                "检查系统应包括3个子系统（编码子系统、剖分子系统、管理子系统）"
                "及代码规范性。如果齐全，则测试通过；如果不齐全，则测试不通过。")


def fill_table_15(tbl):
    rows = tbl.findall(f'{{{W}}}tr')
    entries = [
        ("1", "功能测试/GN",
         "格网定位、格网覆盖、格网拓扑查询、时空编码、各类剖分、任务管理、"
         "配置管理、批次管理、质检报告、数据入库、用户认证",
         "通过Web界面或API调用各项功能，验证输入输出符合需求规格。"
         "若所有功能操作正确、界面响应正常，则测试通过。"),
    ]
    for i, (seq, name, tc, method) in enumerate(entries):
        if i + 1 >= len(rows):
            tr = ET.SubElement(tbl, f'{{{W}}}tr')
            for _ in range(4):
                ET.SubElement(tr, f'{{{W}}}tc')
        else:
            tr = rows[i + 1]
        cells = tr.findall(f'{{{W}}}tc')
        if len(cells) >= 4:
            set_cell_text(cells[0], seq)
            set_cell_text(cells[1], name)
            set_cell_text(cells[2], tc)
            set_cell_text(cells[3], method)
    while len(rows) > len(entries) + 1:
        tbl.remove(rows[-1])
        rows = tbl.findall(f'{{{W}}}tr')


def fill_table_16(tbl):
    rows = tbl.findall(f'{{{W}}}tr')
    entries = [
        ("1", "性能测试/XN", "单景剖分速度(XN-001)、质检速度(XN-002)",
         "单景剖分速度：选择标准光学影像数据提交剖分，记录从提交到完成的时间，要求≤10秒。"
         "质检速度：对已剖分格网数据执行质检，记录每秒处理格网数，要求≥2个格网/秒。"),
    ]
    for i, (seq, name, tc, method) in enumerate(entries):
        if i + 1 >= len(rows):
            tr = ET.SubElement(tbl, f'{{{W}}}tr')
            for _ in range(4):
                ET.SubElement(tr, f'{{{W}}}tc')
        else:
            tr = rows[i + 1]
        cells = tr.findall(f'{{{W}}}tc')
        if len(cells) >= 4:
            set_cell_text(cells[0], seq)
            set_cell_text(cells[1], name)
            set_cell_text(cells[2], tc)
            set_cell_text(cells[3], method)
    while len(rows) > len(entries) + 1:
        tbl.remove(rows[-1])
        rows = tbl.findall(f'{{{W}}}tr')


def fill_table_17(tbl):
    rows = tbl.findall(f'{{{W}}}tr')
    entries = [
        ("1", "接口测试/JK",
         "格网编码SDK(JK-001)、剖分任务API(JK-002)、质检报告API(JK-003)、"
         "配置管理API(JK-004)、数据入库API(JK-005)、认证鉴权API(JK-006)",
         "通过自动化脚本调用各REST API接口，验证请求响应符合接口规范文档。"
         "若HTTP状态码正确、返回数据结构完整、鉴权机制生效，则测试通过。"),
    ]
    for i, (seq, name, tc, method) in enumerate(entries):
        if i + 1 >= len(rows):
            tr = ET.SubElement(tbl, f'{{{W}}}tr')
            for _ in range(4):
                ET.SubElement(tr, f'{{{W}}}tc')
        else:
            tr = rows[i + 1]
        cells = tr.findall(f'{{{W}}}tc')
        if len(cells) >= 4:
            set_cell_text(cells[0], seq)
            set_cell_text(cells[1], name)
            set_cell_text(cells[2], tc)
            set_cell_text(cells[3], method)
    while len(rows) > len(entries) + 1:
        tbl.remove(rows[-1])
        rows = tbl.findall(f'{{{W}}}tr')


def fill_table_38(tbl):
    rows = tbl.findall(f'{{{W}}}tr')
    mappings = [
        (1, "数据检索接口/LZ006-YW-03-JK-NBJK-002",
         "格网定位功能/LZ006-YW-02-04-GN-001,格网覆盖功能/LZ006-YW-02-04-GN-002"),
        (2, "影像数据服务接口/LZ006-YW-03-JK-NBJK-003",
         "光学影像剖分/LZ006-YW-02-04-GN-005,光学影像实体剖分/LZ006-YW-02-04-GN-006"),
        (3, "数据服务接口/LZ006-YW-03-JK-NBJK-004",
         "雷达数据剖分/LZ006-YW-02-04-GN-007,雷达数据实体剖分/LZ006-YW-02-04-GN-008"),
        (4, "数据服务接口/LZ006-YW-03-JK-NBJK-005",
         "产品数据剖分/LZ006-YW-02-04-GN-009,产品数据实体剖分/LZ006-YW-02-04-GN-010"),
        (5, "数据服务接口/LZ006-YW-03-JK-NBJK-006",
         "碳卫星数据剖分/LZ006-YW-02-04-GN-011"),
    ]
    for i, (seq, iface, tc) in enumerate(mappings):
        if i + 1 >= len(rows):
            tr = ET.SubElement(tbl, f'{{{W}}}tr')
            for _ in range(4):
                ET.SubElement(tr, f'{{{W}}}tc')
        else:
            tr = rows[i + 1]
        cells = tr.findall(f'{{{W}}}tc')
        if len(cells) >= 4:
            set_cell_text(cells[0], str(seq))
            set_cell_text(cells[1], iface)
            set_cell_text(cells[2], tc)
    while len(rows) > len(mappings) + 1:
        tbl.remove(rows[-1])
        rows = tbl.findall(f'{{{W}}}tr')


def main():
    src = Path("/home/lyajun/projects/cube_project/docs/分析就绪数据剖分管理系统测试大纲.docx")
    dst = Path("/home/lyajun/projects/cube_project/docs/分析就绪数据剖分管理系统测试大纲_v2.docx")

    extract_dir = Path("/tmp/docx_update2")
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir()
    with zipfile.ZipFile(src, 'r') as z:
        z.extractall(extract_dir)

    doc_path = extract_dir / "word" / "document.xml"
    tree = ET.parse(doc_path)
    root = tree.getroot()
    body = root.find(f'{{{W}}}body')

    all_tables = body.findall(f'{{{W}}}tbl')
    print(f"Total tables: {len(all_tables)}")

    if len(all_tables) > 14:
        print("Filling Table 14: 齐套性检查方法...")
        fill_table_14(all_tables[14])
    if len(all_tables) > 15:
        print("Filling Table 15: 功能测试方法...")
        fill_table_15(all_tables[15])
    if len(all_tables) > 16:
        print("Filling Table 16: 性能测试方法...")
        fill_table_16(all_tables[16])
    if len(all_tables) > 17:
        print("Filling Table 17: 接口测试方法...")
        fill_table_17(all_tables[17])
    if len(all_tables) > 18:
        print("Filling Table 18: 测试用例总表...")
        fill_table_18(all_tables[18])
    if len(all_tables) > 38:
        print("Filling Table 38: 内部接口映射...")
        fill_table_38(all_tables[38])

    tree.write(doc_path, xml_declaration=True, encoding='UTF-8')

    with zipfile.ZipFile(dst, 'w', zipfile.ZIP_DEFLATED) as zf:
        for f in extract_dir.rglob('*'):
            if f.is_file():
                arcname = f.relative_to(extract_dir)
                zf.write(f, arcname)

    print(f"\nDone! Output: {dst}")


if __name__ == '__main__':
    main()
