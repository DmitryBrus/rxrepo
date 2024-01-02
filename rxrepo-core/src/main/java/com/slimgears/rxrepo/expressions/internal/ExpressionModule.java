package com.slimgears.rxrepo.expressions.internal;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.deser.Deserializers;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.Serializers;
import com.google.auto.service.AutoService;
import com.google.common.reflect.TypeToken;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeTokens;

import java.io.IOException;

@SuppressWarnings({"UnstableApiUsage", "rawtypes"})
@AutoService(Module.class)
public class ExpressionModule extends Module {
    @Override
    public String getModuleName() {
        return getClass().getName();
    }

    @Override
    public Version version() {
        return new Version(1, 0, 0, null, null, null);
    }

    @Override
    public void setupModule(SetupContext setupContext) {
        setupContext.addSerializers(createSerializers());
        setupContext.addDeserializers(createDeserializers());
    }

    private Deserializers createDeserializers() {
        SimpleDeserializers simpleDeserializers = new SimpleDeserializers();
        simpleDeserializers.addDeserializer(TypeToken.class, new JsonDeserializer<TypeToken>() {
            @Override
            public TypeToken deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                return TypeTokens.valueOf(p.getText());
            }
        });
        simpleDeserializers.addDeserializer(PropertyMeta.class, new JsonDeserializer<PropertyMeta>() {
            @Override
            public PropertyMeta deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                JsonNode treeNode = p.readValueAsTree();
                TypeToken declaredType = TypeTokens.valueOf(treeNode.get("type").asText());
                String name = treeNode.get("name").asText();
                //noinspection unchecked
                return MetaClasses.forToken(declaredType).getProperty(name);
            }
        });
        return simpleDeserializers;
    }

    private Serializers createSerializers() {
        SimpleSerializers simpleSerializers = new SimpleSerializers();
        simpleSerializers.addSerializer(TypeToken.class, new JsonSerializer<TypeToken>() {
            @SuppressWarnings("unchecked")
            @Override
            public void serialize(TypeToken value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeString(TypeTokens.eliminateTypeVars(value).toString());
            }
        });
        simpleSerializers.addSerializer(PropertyMeta.class, new JsonSerializer<PropertyMeta>() {
            @SuppressWarnings("unchecked")
            @Override
            public void serialize(PropertyMeta value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeStartObject();
                gen.writeFieldName("type");
                gen.writeString(TypeTokens.eliminateTypeVars(value.declaringType().asType()).toString());
                gen.writeFieldName("name");
                gen.writeString(value.name());
                gen.writeEndObject();
            }
        });
        return simpleSerializers;
    }
}
