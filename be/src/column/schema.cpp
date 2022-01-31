// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/schema.h"

#include <algorithm>

namespace starrocks::vectorized {

Schema::Schema(Fields fields) : _fields(std::move(fields)) {
    auto is_key = [](const FieldPtr& f) { return f->is_key(); };
    _num_keys = std::count_if(_fields.begin(), _fields.end(), is_key);
    _build_index_map(_fields);
}

void Schema::append(const FieldPtr& field) {
    _fields.emplace_back(field);
    _put_index(field, _fields.size() - 1);
    _num_keys += field->is_key();
}

void Schema::insert(size_t idx, const FieldPtr& field) {
    DCHECK_LT(idx, _fields.size());

    _fields.emplace(_fields.begin() + idx, field);
    _name_to_index.clear();
    _num_keys += field->is_key();
    _build_index_map(_fields);
}

void Schema::remove(size_t idx) {
    DCHECK_LT(idx, _fields.size());
    _num_keys -= _fields[idx]->is_key();
    if (idx == _fields.size() - 1) {
        _remove_index(_fields[idx]);
        _fields.erase(_fields.begin() + idx);
    } else {
        _fields.erase(_fields.begin() + idx);
        _name_to_index.clear();
        _build_index_map(_fields);
    }
}

void Schema::set_fields(Fields fields) {
    _fields = std::move(fields);
    auto is_key = [](const FieldPtr& f) { return f->is_key(); };
    _num_keys = std::count_if(_fields.begin(), _fields.end(), is_key);
    _build_index_map(_fields);
}

const FieldPtr& Schema::field(size_t idx) const {
    DCHECK_GE(idx, 0);
    DCHECK_LT(idx, _fields.size());
    return _fields[idx];
}

std::vector<std::string> Schema::field_names() const {
    std::vector<std::string> names;
    names.reserve(_fields.size());
    for (const auto& field : _fields) {
        names.emplace_back(field->name());
    }
    return names;
}

FieldPtr Schema::get_field_by_name(const std::string& name) const {
    size_t idx = get_field_index_by_name(name);
    return idx == -1 ? nullptr : _fields[idx];
}

void Schema::_build_index_map(const Fields& fields) {
    for (size_t i = 0; i < fields.size(); i++) {
        _put_index(fields[i], i);
    }
}

void Schema::_put_index(const FieldPtr& field, int i) {
    _name_to_index.emplace(field->mutable_name(), i);
    if (field->aggregate_method() == OLAP_FIELD_AGGREGATION_NONE ||
        field->aggregate_method() == OLAP_FIELD_AGGREGATION_UNKNOWN) {
        return;
    }
    std::stringstream ss;
    ss << field->mutable_name() << "#" << aggregation_method_to_string(field->aggregate_method());
    _name_to_index.emplace(ss.str(), i);
}

void Schema::_remove_index(const FieldPtr& field) {
    _name_to_index.erase(field->mutable_name());
    if (field->aggregate_method() == OLAP_FIELD_AGGREGATION_NONE ||
        field->aggregate_method() == OLAP_FIELD_AGGREGATION_UNKNOWN) {
        return;
    }
    std::stringstream ss;
    ss << field->mutable_name() << "#" << aggregation_method_to_string(field->aggregate_method());
    _name_to_index.erase(ss.str());
}

size_t Schema::get_field_index_by_name(const std::string& name) const {
    auto p = _name_to_index.find(name);
    if (p == _name_to_index.end()) {
        return -1;
    }
    return p->second;
}

size_t Schema::get_field_index_by_name_fn(const std::string& name, const std::string& agg_fn_name) const {
    if (agg_fn_name == "") {
        return get_field_index_by_name(name);
    }
    std::string upper_fn_name = agg_fn_name;
    std::transform(upper_fn_name.begin(), upper_fn_name.end(), upper_fn_name.begin(), ::toupper);
    auto p = _name_to_index.find(name + "#" + upper_fn_name);
    if (p == _name_to_index.end()) {
        return -1;
    }
    return p->second;
}

} // namespace starrocks::vectorized