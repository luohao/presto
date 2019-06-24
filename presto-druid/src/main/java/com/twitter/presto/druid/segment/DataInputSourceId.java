package com.twitter.presto.druid.segment;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DataInputSourceId
{
    private final String id;

    public DataInputSourceId(String id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataInputSourceId that = (DataInputSourceId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public String toString()
    {
        return id;
    }
}
