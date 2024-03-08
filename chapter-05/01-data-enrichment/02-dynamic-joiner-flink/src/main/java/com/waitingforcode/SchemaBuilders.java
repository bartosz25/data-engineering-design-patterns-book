package com.waitingforcode;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaBuilders {

    public static List<Schema.UnresolvedColumn> forVisits() {
        return Arrays.asList(
                new Schema.UnresolvedPhysicalColumn("visit_id", DataTypes.STRING().notNull()),
                new Schema.UnresolvedPhysicalColumn("event_time", DataTypes.TIMESTAMP_LTZ(3).notNull()),
                new Schema.UnresolvedPhysicalColumn("page", DataTypes.STRING().notNull())
        );
    }

    public static List<Schema.UnresolvedColumn> forAds() {
        return Arrays.asList(
                new Schema.UnresolvedPhysicalColumn("ad_page", DataTypes.STRING().notNull()),
                new Schema.UnresolvedPhysicalColumn("campaign_name", DataTypes.STRING().notNull()),
                new Schema.UnresolvedPhysicalColumn("update_time", DataTypes.TIMESTAMP_LTZ(3).notNull())
        );
    }

    public static List<Schema.UnresolvedColumn> forVisitsWithAds() {
        return Stream.concat(
                    forVisits().stream(),
                    forAds().stream()
                )
                .collect(Collectors.toList());
    }

}
