from types import NoneType
from unittest import TestCase

from src.pyodb.schema.base._operators import Disassembler
from test.test_models.primitive_models import (
    PrimitiveBasic,
    PrimitiveContainer,
    PrimitiveIllegal1,
)
from test.test_models.complex_models import (
    ComplexBasic,
    ComplexIllegal1,
    ComplexIllegal2,
    ComplexIllegal3,
    ComplexMulti,
    ComplexContainer,
)


class DissassemblerTest(TestCase):
    def setUp(self) -> None:
        Disassembler.max_depth = 3
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
                TypeError,
                Disassembler.disassemble_type,
                type_
            )


    def test_disassembly_depth(self):
        Disassembler.max_depth = 0
        self.assertRaises(RecursionError, Disassembler.disassemble_type, ComplexBasic)
