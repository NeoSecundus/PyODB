from pathlib import Path
from test.test_models.complex_models import (ComplexBasic, ComplexContainer, ComplexIllegal1,
                                             ComplexIllegal2, ComplexIllegal3, ComplexMulti)
from test.test_models.primitive_models import (PrimitiveBasic, PrimitiveContainer,
                                               PrimitiveIllegal1, ReassemblyTester)
from types import NoneType
from unittest import TestCase

from src.pyodb.error import DBConnError, DisassemblyError, MixedTypesError
from src.pyodb.schema.base._operators import Assembler, Disassembler
from src.pyodb.schema.unified_schema import UnifiedSchema


class DissassemblerTest(TestCase):
    def setUp(self) -> None:
        return super().setUp()


    def test_primitive_disassembly(self):
        tables = Disassembler.disassemble_type(PrimitiveBasic)
        self.assertEqual(
            tables[0].members,
            PrimitiveBasic.get_members()
        )

        tables = Disassembler.disassemble_type(PrimitiveContainer)
        self.assertEqual(
            tables[0].members,
            PrimitiveContainer.get_members()
        )


    def test_complex_disassembly(self):
        tables = Disassembler.disassemble_type(ComplexBasic)
        self.assertEqual(
            tables[0].members,
            ComplexBasic.get_members()
        )
        self.assertEqual(
            tables[1].members,
            PrimitiveBasic.get_members()
        )
        self.assertEqual(
            tables[2].members,
            PrimitiveContainer.get_members()
        )

        tables = Disassembler.disassemble_type(ComplexMulti)
        self.assertEqual(
            tables[0].members,
            ComplexMulti.get_members()
        )
        self.assertEqual(
            tables[1].members,
            PrimitiveBasic.get_members()
        )
        self.assertEqual(
            tables[2].members,
            PrimitiveContainer.get_members()
        )

        tables = Disassembler.disassemble_type(ComplexContainer)
        self.assertEqual(
            tables[0].members,
            ComplexContainer.get_members()
        )


    def test_disassembly_errors(self):
        bad_types = [
            PrimitiveBasic(),
            2.1,
            float,
            str,
            int | float,
            str | None,
            NoneType,
            PrimitiveIllegal1,
            ComplexIllegal1,
            ComplexIllegal2,
            ComplexIllegal3,
        ]

        for type_ in bad_types:
            print(f"Testing type: {type_}")
            self.assertRaises(
                (DisassemblyError, MixedTypesError),
                Disassembler.disassemble_type,
                type_
            )


class AssemblerTest(TestCase):
    def test_assembly_connection_error(self):
        schema = UnifiedSchema(Path(".pyodb"), 1, False)
        schema.add_type(ComplexBasic)
        pbs = [ComplexBasic() for _ in range(10)]
        schema.insert_many(pbs, None)

        table = schema._tables[ComplexBasic]
        if not table.dbconn:
            self.fail()

        row = table.dbconn.execute(f"SELECT * FROM \"{table.fqcn}\" LIMIT 1;").fetchone()
        schema._tables[PrimitiveBasic].dbconn = None
        self.assertRaises(
            DBConnError,
            Assembler.assemble_types,
            base_type=ComplexBasic,
            tables=schema._tables,
            rows=[row]
        )
        del schema


    def test_reassembly_function(self):
        schema = UnifiedSchema(Path(".pyodb"), 1, False)
        schema.add_type(ReassemblyTester)
        rts = [ReassemblyTester() for _ in range(10)]
        schema.insert_many(rts, None)

        table = schema._tables[ReassemblyTester]
        if not table.dbconn:
            self.fail()

        row = table.dbconn.execute(f"SELECT * FROM \"{table.fqcn}\" LIMIT 1;").fetchone()
        res = Assembler.assemble_types(ReassemblyTester, schema._tables, [row])
        self.assertTrue(res[0].reassembled)
        del schema
