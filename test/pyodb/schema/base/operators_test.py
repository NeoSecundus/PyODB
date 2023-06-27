from pathlib import Path
from test.test_models.complex_models import (ComplexBasic, ComplexContainer, ComplexIllegal1,
                                             ComplexIllegal2, ComplexIllegal3, ComplexMulti, ComplexPydantic)
from test.test_models.primitive_models import (PrimitiveBasic, PrimitiveContainer,
                                               PrimitiveIllegal1, PrimitiveIllegal2, PrimitivePydantic,
                                               ReassemblyTester)
from types import NoneType
from unittest import TestCase

from pyodb.error import DisassemblyError, MixedTypesError
from pyodb.schema.base._operators import Assembler, Disassembler
from pyodb.schema.unified_schema import UnifiedSchema


class DissassemblerTest(TestCase):
    def setUp(self) -> None:
        return super().setUp()


    def test_primitive_disassembly(self):
        ttypes = Disassembler.disassemble_type(PrimitiveBasic)
        self.assertEqual(
            ttypes[PrimitiveBasic],
            PrimitiveBasic.get_members()
        )

        ttypes = Disassembler.disassemble_type(PrimitiveContainer)
        self.assertEqual(
            ttypes[PrimitiveContainer],
            PrimitiveContainer.get_members()
        )

        ttypes = Disassembler.disassemble_type(PrimitivePydantic)
        self.assertEqual(
            ttypes[PrimitivePydantic],
            PrimitivePydantic.get_members()
        )


    def test_complex_disassembly(self):
        ttypes = Disassembler.disassemble_type(ComplexBasic)
        self.assertEqual(
            ttypes[ComplexBasic],
            ComplexBasic.get_members()
        )
        self.assertEqual(
            ttypes[PrimitiveBasic],
            PrimitiveBasic.get_members()
        )
        self.assertEqual(
            ttypes[PrimitiveContainer],
            PrimitiveContainer.get_members()
        )

        ttypes = Disassembler.disassemble_type(ComplexMulti)
        self.assertEqual(
            ttypes[ComplexMulti],
            ComplexMulti.get_members()
        )
        self.assertEqual(
            ttypes[PrimitiveBasic],
            PrimitiveBasic.get_members()
        )
        self.assertEqual(
            ttypes[PrimitiveContainer],
            PrimitiveContainer.get_members()
        )

        ttypes = Disassembler.disassemble_type(ComplexContainer)
        self.assertEqual(
            ttypes[ComplexContainer],
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
            PrimitiveIllegal2,
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
    def test_pydantic_reassembly(self):
        schema = UnifiedSchema(Path(".pyodb"), 1, False)
        schema.add_type(ComplexPydantic)
        schema.insert(
            ComplexPydantic(
                child=PrimitivePydantic(
                test_float=1.5,
                test_str="Hello Child",
                test_number=4,
                test_bool=True
                ),
                test_str="Hello"
            ),
            expires=None
        )

        table = schema._tables[ComplexPydantic]
        row = table.dbconn.execute(f"SELECT * FROM \"{table.fqcn}\" LIMIT 1;").fetchone()
        res: ComplexPydantic = Assembler.assemble_types(
            ComplexPydantic,
            schema._tables,
            [row]
        )[0]
        self.assertEqual(res.test_str, "Hello")
        self.assertEqual(res.child.test_str, "Hello Child")
        self.assertEqual(res.child.test_float, 1.5)
        self.assertEqual(res.child.test_number, 4)
        self.assertTrue(res.child.test_bool)
        del schema


    def test_reassembly_function(self):
        schema = UnifiedSchema(Path(".pyodb"), 1, False)
        schema.add_type(ReassemblyTester)
        rts = [ReassemblyTester() for _ in range(10)]
        schema.insert_many(rts, None)

        table = schema._tables[ReassemblyTester]

        row = table.dbconn.execute(f"SELECT * FROM \"{table.fqcn}\" LIMIT 1;").fetchone()
        res = Assembler.assemble_types(ReassemblyTester, schema._tables, [row])
        self.assertTrue(res[0].reassembled)
        del schema
