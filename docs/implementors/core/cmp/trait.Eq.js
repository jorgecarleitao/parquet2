(function() {var implementors = {};
implementors["parquet2"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/error/enum.Feature.html\" title=\"enum parquet2::error::Feature\">Feature</a>","synthetic":false,"types":["parquet2::error::Feature"]},{"text":"impl&lt;'a&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/encoding/hybrid_rle/enum.HybridEncoded.html\" title=\"enum parquet2::encoding::hybrid_rle::HybridEncoded\">HybridEncoded</a>&lt;'a&gt;","synthetic":false,"types":["parquet2::encoding::hybrid_rle::HybridEncoded"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for dyn <a class=\"trait\" href=\"parquet2/indexes/trait.Index.html\" title=\"trait parquet2::indexes::Index\">Index</a> + '_","synthetic":false,"types":[]},{"text":"impl&lt;T:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> + <a class=\"trait\" href=\"parquet2/types/trait.NativeType.html\" title=\"trait parquet2::types::NativeType\">NativeType</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"struct\" href=\"parquet2/indexes/struct.NativeIndex.html\" title=\"struct parquet2::indexes::NativeIndex\">NativeIndex</a>&lt;T&gt;","synthetic":false,"types":["parquet2::indexes::index::NativeIndex"]},{"text":"impl&lt;T:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"struct\" href=\"parquet2/indexes/struct.PageIndex.html\" title=\"struct parquet2::indexes::PageIndex\">PageIndex</a>&lt;T&gt;","synthetic":false,"types":["parquet2::indexes::index::PageIndex"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"struct\" href=\"parquet2/indexes/struct.ByteIndex.html\" title=\"struct parquet2::indexes::ByteIndex\">ByteIndex</a>","synthetic":false,"types":["parquet2::indexes::index::ByteIndex"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"struct\" href=\"parquet2/indexes/struct.FixedLenByteIndex.html\" title=\"struct parquet2::indexes::FixedLenByteIndex\">FixedLenByteIndex</a>","synthetic":false,"types":["parquet2::indexes::index::FixedLenByteIndex"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"struct\" href=\"parquet2/indexes/struct.BooleanIndex.html\" title=\"struct parquet2::indexes::BooleanIndex\">BooleanIndex</a>","synthetic":false,"types":["parquet2::indexes::index::BooleanIndex"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"struct\" href=\"parquet2/indexes/struct.Interval.html\" title=\"struct parquet2::indexes::Interval\">Interval</a>","synthetic":false,"types":["parquet2::indexes::intervals::Interval"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/indexes/enum.FilteredPage.html\" title=\"enum parquet2::indexes::FilteredPage\">FilteredPage</a>","synthetic":false,"types":["parquet2::indexes::intervals::FilteredPage"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"struct\" href=\"parquet2/metadata/struct.Descriptor.html\" title=\"struct parquet2::metadata::Descriptor\">Descriptor</a>","synthetic":false,"types":["parquet2::metadata::column_descriptor::Descriptor"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/schema/enum.Repetition.html\" title=\"enum parquet2::schema::Repetition\">Repetition</a>","synthetic":false,"types":["parquet2::parquet_bridge::Repetition"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/compression/enum.Compression.html\" title=\"enum parquet2::compression::Compression\">Compression</a>","synthetic":false,"types":["parquet2::parquet_bridge::Compression"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/page/enum.PageType.html\" title=\"enum parquet2::page::PageType\">PageType</a>","synthetic":false,"types":["parquet2::parquet_bridge::PageType"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/encoding/enum.Encoding.html\" title=\"enum parquet2::encoding::Encoding\">Encoding</a>","synthetic":false,"types":["parquet2::parquet_bridge::Encoding"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/indexes/enum.BoundaryOrder.html\" title=\"enum parquet2::indexes::BoundaryOrder\">BoundaryOrder</a>","synthetic":false,"types":["parquet2::parquet_bridge::BoundaryOrder"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/schema/types/enum.PhysicalType.html\" title=\"enum parquet2::schema::types::PhysicalType\">PhysicalType</a>","synthetic":false,"types":["parquet2::schema::types::physical_type::PhysicalType"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"struct\" href=\"parquet2/schema/types/struct.FieldInfo.html\" title=\"struct parquet2::schema::types::FieldInfo\">FieldInfo</a>","synthetic":false,"types":["parquet2::schema::types::basic_type::FieldInfo"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/schema/types/enum.PrimitiveConvertedType.html\" title=\"enum parquet2::schema::types::PrimitiveConvertedType\">PrimitiveConvertedType</a>","synthetic":false,"types":["parquet2::schema::types::converted_type::PrimitiveConvertedType"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"struct\" href=\"parquet2/schema/types/struct.PrimitiveType.html\" title=\"struct parquet2::schema::types::PrimitiveType\">PrimitiveType</a>","synthetic":false,"types":["parquet2::schema::types::parquet_type::PrimitiveType"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"struct\" href=\"parquet2/write/struct.WriteOptions.html\" title=\"struct parquet2::write::WriteOptions\">WriteOptions</a>","synthetic":false,"types":["parquet2::write::WriteOptions"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.59.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for <a class=\"enum\" href=\"parquet2/write/enum.Version.html\" title=\"enum parquet2::write::Version\">Version</a>","synthetic":false,"types":["parquet2::write::Version"]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()