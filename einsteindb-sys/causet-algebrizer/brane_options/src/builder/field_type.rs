//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use einsteindbpb::FieldType;

use crate::FieldTypeAccessor;

/// Helper to build a `FieldType` protobuf message.
#[derive(Default)]
pub struct FieldTypeBuilder(FieldType);

impl FieldTypeBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn tp(mut self, v: crate::FieldTypeTp) -> Self {
        FieldTypeAccessor::set_tp(&mut self.0, v);
        self
    }

    pub fn flag(mut self, v: crate::FieldTypeFlag) -> Self {
        FieldTypeAccessor::set_flag(&mut self.0, v);
        self
    }

    pub fn flen(mut self, v: isize) -> Self {
        FieldTypeAccessor::set_flen(&mut self.0, v);
        self
    }

    pub fn decimal(mut self, v: isize) -> Self {
        FieldTypeAccessor::set_decimal(&mut self.0, v);
        self
    }

    pub fn collation(mut self, v: crate::Collation) -> Self {
        FieldTypeAccessor::set_collation(&mut self.0, v);
        self
    }

    pub fn build(self) -> FieldType {
        self.0
    }
}

impl From<FieldTypeBuilder> for FieldType {
    fn from(fp_builder: FieldTypeBuilder) -> FieldType {
        fp_builder.build()
    }
}
