# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: definitons/visit_v2.proto
# Protobuf Python Version: 5.27.2
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    27,
    2,
    '',
    'definitons/visit_v2.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x19\x64\x65\x66initons/visit_v2.proto\x12\x12\x63om.waitingforcode\x1a\x1fgoogle/protobuf/timestamp.proto\"f\n\x0bUserDetails\x12\x0e\n\x02id\x18\x01 \x01(\tR\x02id\x12\x0e\n\x02ip\x18\x02 \x01(\tR\x02ip\x12\x14\n\x05login\x18\x03 \x01(\tR\x05login\x12!\n\x0cis_connected\x18\x04 \x01(\x08R\x0bisConnected\"\xd3\x01\n\x07VisitV2\x12\x19\n\x08visit_id\x18\x01 \x01(\tR\x07visitId\x12\x39\n\nevent_time\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.TimestampR\teventTime\x12\x12\n\x04page\x18\x04 \x01(\tR\x04page\x12\x1a\n\x08referral\x18\x08 \x01(\tR\x08referral\x12\x42\n\x0cuser_details\x18\t \x01(\x0b\x32\x1f.com.waitingforcode.UserDetailsR\x0buserDetailsB\x8f\x01\n\x16\x63om.com.waitingforcodeB\x0cVisitV2ProtoP\x01\xa2\x02\x03\x43WX\xaa\x02\x12\x43om.Waitingforcode\xca\x02\x12\x43om\\Waitingforcode\xe2\x02\x1e\x43om\\Waitingforcode\\GPBMetadata\xea\x02\x13\x43om::Waitingforcodeb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'definitons.visit_v2_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n\026com.com.waitingforcodeB\014VisitV2ProtoP\001\242\002\003CWX\252\002\022Com.Waitingforcode\312\002\022Com\\Waitingforcode\342\002\036Com\\Waitingforcode\\GPBMetadata\352\002\023Com::Waitingforcode'
  _globals['_USERDETAILS']._serialized_start=82
  _globals['_USERDETAILS']._serialized_end=184
  _globals['_VISITV2']._serialized_start=187
  _globals['_VISITV2']._serialized_end=398
# @@protoc_insertion_point(module_scope)
