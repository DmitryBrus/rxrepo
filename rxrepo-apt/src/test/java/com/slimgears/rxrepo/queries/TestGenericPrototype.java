package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseAutoValueAnnotator;
import com.slimgears.util.autovalue.annotations.UseBuilderExtension;

@AutoValuePrototype
@UseFilters
@UseAutoValueAnnotator
@UseBuilderExtension
public interface TestGenericPrototype<T> {
    @Filterable T value();
}
