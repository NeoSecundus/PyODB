from unittest import TestCase

from src.pyodb.schema._base_schema import BaseSchema
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer, PrimitiveIllegal
from test.test_models.complex_models import ComplexBasic, ComplexIllegal1, ComplexIllegal2,\
ComplexIllegal3, ComplexMulti, ComplexContainer


class BaseSchemaTest(TestCase):
    def setUp(self) -> None:
        self.schema = BaseSchema(None, 2)
        return super().setUp()


    def test_primitive_disassembly(self):
        tables = self.schema._disassemble_type(PrimitiveBasic)
        self.assertEqual(
            tables[0].members,
            PrimitiveBasic.get_members()
        )

        tables = self.schema._disassemble_type(PrimitiveContainer)
        self.assertEqual(
            tables[0].members,
            PrimitiveContainer.get_members()
        )


    def test_complex_disassembly(self):
        tables = self.schema._disassemble_type(ComplexBasic)
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

        tables = self.schema._disassemble_type(ComplexMulti)
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

        tables = self.schema._disassemble_type(ComplexContainer)
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
            PrimitiveIllegal,
            ComplexIllegal1,
            ComplexIllegal2,
            ComplexIllegal3
        ]

        for type_ in bad_types:
            print(f"Testing {type_}")
            self.assertRaises(
                TypeError,
                self.schema._disassemble_type,
                type_
            )


    def test_disassembly_depth(self):
        self.schema._max_depth = 0
        tables = self.schema._disassemble_type(ComplexBasic)
        self.assertEqual(len(tables), 1)


    def test_schema_size(self):
        tables = self.schema._disassemble_type(ComplexBasic)
        for table in tables:
            self.schema._tables[table.base_type] = table
        self.assertEqual(self.schema.schema_size, 3)
        self.assertTrue(self.schema.is_known_type(PrimitiveBasic))
        self.assertFalse(self.schema.is_known_type(ComplexMulti))


    def test_not_implemented(self):
        self.assertRaises(NotImplementedError, self.schema.add_type, PrimitiveBasic)
        self.assertRaises(NotImplementedError, self.schema.remove_type, PrimitiveBasic)
