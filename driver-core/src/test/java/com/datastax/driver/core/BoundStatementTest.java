package com.datastax.driver.core;

import java.util.Collection;
import java.util.Collections;

import com.google.common.collect.Lists;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.exceptions.InvalidTypeException;

public class BoundStatementTest extends CCMBridge.PerClassSingleNodeCluster {

    PreparedStatement prepared;

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE foo (k int primary key, v1 text, v2 list<int>)");
    }

    @BeforeClass(groups = "short")
    public void setup() {
        prepared = session.prepare("INSERT INTO foo (k, v1, v2) VALUES (?, ?, ?)");
    }

    @Test(groups = "short")
    public void should_get_single_value() {
        // This test is not exhaustive, note that the method is also covered in DataTypeIntegrationTest.
        BoundStatement statement = prepared.bind(1, "test", Lists.newArrayList(1));

        assertThat(statement.getInt(0))
            .isEqualTo(statement.getInt("k"))
            .isEqualTo(1);

        assertThat(statement.getString(1))
            .isEqualTo(statement.getString("v1"))
            .isEqualTo("test");

        assertThat(statement.getList(2, Integer.class))
            .isEqualTo(statement.getList("v2", Integer.class))
            .isEqualTo(Lists.newArrayList(1));

        try {
            statement.getString(0);
            fail("Expected type error");
        } catch (InvalidTypeException e) { /* expected */ }

        try {
            statement.getString(3);
            fail("Expected index error");
        } catch (IndexOutOfBoundsException e) { /* expected */ }
    }

    @Test(groups = "short")
    public void should_get_all_values() {
        BoundStatement statement = prepared.bind(1, "test", Lists.newArrayList(1));
        assertThat(statement.getValues()).containsExactly(1, "test", Lists.newArrayList(1));
    }

    @Test(groups = "short")
    public void should_get_all_values_when_not_all_set() {
        BoundStatement statement = prepared.bind();
        assertThat(statement.getValues()).containsExactly(null, null, Collections.emptyList());

        statement = prepared.bind();
        statement.setInt(0, 1);
        assertThat(statement.getValues()).containsExactly(1, null, Collections.emptyList());

        statement = prepared.bind();
        statement.setString(1, "test");
        assertThat(statement.getValues()).containsExactly(null, "test", Collections.emptyList());

        statement = prepared.bind();
        statement.setList(2, Lists.newArrayList(1));
        assertThat(statement.getValues()).containsExactly(null, null, Lists.newArrayList(1));
    }
}