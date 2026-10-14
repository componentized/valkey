//! The geo commands: GEOADD, GEODIST, GEOPOS, GEOSEARCH and the like.

use crate::command::{Flag, IntoCommand, NonEmpty, NumKeys, Words};
use crate::types::{
    Double, Error, GeoCoordinates, GeoUnit, GeoaddCondition, GeosearchCount, GeosearchFrom,
    GeosearchShape, GeosearchWithResult, RespValue, StringOrBytes,
};
use crate::value::{FromValue, items, unexpected};

/// `longitude latitude`
impl IntoCommand for GeoCoordinates {
    fn into_command(self) -> Result<Words, Error> {
        (self.longitude, self.latitude).into_command()
    }
}

/// `[longitude, latitude]`
impl FromValue for GeoCoordinates {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let (longitude, latitude) = FromValue::from_value(value)?;
        Ok(GeoCoordinates {
            longitude,
            latitude,
        })
    }
}

impl IntoCommand for GeoUnit {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            GeoUnit::M => "m",
            GeoUnit::Km => "km",
            GeoUnit::Ft => "ft",
            GeoUnit::Mi => "mi",
        }
        .into_command()
    }
}

impl IntoCommand for GeoaddCondition {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            GeoaddCondition::Nx => "NX",
            GeoaddCondition::Xx => "XX",
        }
        .into_command()
    }
}

impl IntoCommand for GeosearchFrom {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            GeosearchFrom::Member(member) => ("FROMMEMBER", member).into_command(),
            GeosearchFrom::Lonlat(coordinates) => ("FROMLONLAT", coordinates).into_command(),
        }
    }
}

/// The center, when the shape has one, then the shape. Also `GeosearchstoreShape`, the same type.
impl IntoCommand for GeosearchShape {
    fn into_command(self) -> Result<Words, Error> {
        match self {
            GeosearchShape::Radius(circle) => {
                (circle.from, "BYRADIUS", circle.radius, circle.unit).into_command()
            }
            GeosearchShape::Box(rectangle) => (
                rectangle.from,
                "BYBOX",
                rectangle.width,
                rectangle.height,
                rectangle.unit,
            )
                .into_command(),
            GeosearchShape::Polygon(vertices) => {
                ("BYPOLYGON", NumKeys(NonEmpty::new("vertices", vertices))).into_command()
            }
        }
    }
}

/// `COUNT count [ ANY ]`, also `GeosearchstoreCount`, the same type.
impl IntoCommand for GeosearchCount {
    fn into_command(self) -> Result<Words, Error> {
        ("COUNT", self.count, Flag("ANY", self.any)).into_command()
    }
}

/// The member, or with any of WITHDIST, WITHHASH and WITHCOORD, an array of the member then the
/// details, in that order, told apart by their types.
impl FromValue for GeosearchWithResult {
    fn from_value(value: RespValue) -> Result<Self, Error> {
        let mut result = GeosearchWithResult {
            member: StringOrBytes::String(String::new()),
            distance: None,
            hash: None,
            coordinates: None,
        };
        let items = match value {
            RespValue::Array(_) => items(value)?,
            // without details, the member alone
            value => vec![value],
        };
        let mut items = items.into_iter();
        result.member = StringOrBytes::from_value(items.next().unwrap_or(RespValue::Null))?;
        for item in items {
            match item {
                // a bulk string, as RESP3 has no other way to send a fixed precision double
                RespValue::BulkString(_) => result.distance = Some(Double::from_value(item)?),
                RespValue::Integer(hash) => result.hash = Some(hash),
                RespValue::Array(_) => result.coordinates = Some(GeoCoordinates::from_value(item)?),
                item => return Err(unexpected(&item)),
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{array, bulk, bulks, from, from_error, int, words, words_error};
    use crate::types::{GeosearchBox, GeosearchRadius};

    fn coordinates(longitude: f64, latitude: f64) -> GeoCoordinates {
        GeoCoordinates {
            longitude,
            latitude,
        }
    }

    #[test]
    fn geo_coordinates() {
        assert_eq!(
            words(coordinates(13.361389, 38.115556)),
            ["13.361389", "38.115556"]
        );
        assert_eq!(
            from::<GeoCoordinates>(bulks(&["13.5", "38.25"])),
            coordinates(13.5, 38.25)
        );
        assert_eq!(
            from_error::<GeoCoordinates>(bulks(&["13.5"])),
            "unexpected number of items, expected 2: 1"
        );
    }

    #[test]
    fn keywords() {
        assert_eq!(
            words([GeoUnit::M, GeoUnit::Km, GeoUnit::Ft, GeoUnit::Mi]),
            ["m", "km", "ft", "mi"]
        );
        assert_eq!(
            words([GeoaddCondition::Nx, GeoaddCondition::Xx]),
            ["NX", "XX"]
        );
    }

    #[test]
    fn geosearch_from() {
        assert_eq!(
            words(GeosearchFrom::Member("Palermo".into())),
            ["FROMMEMBER", "Palermo"]
        );
        assert_eq!(
            words(GeosearchFrom::Lonlat(coordinates(15.0, 37.0))),
            ["FROMLONLAT", "15", "37"]
        );
    }

    #[test]
    fn geosearch_shape() {
        let radius = GeosearchShape::Radius(GeosearchRadius {
            from: GeosearchFrom::Member("Palermo".into()),
            radius: 200.5,
            unit: GeoUnit::Km,
        });
        assert_eq!(
            words(radius),
            ["FROMMEMBER", "Palermo", "BYRADIUS", "200.5", "km"]
        );
        let rectangle = GeosearchShape::Box(GeosearchBox {
            from: GeosearchFrom::Lonlat(coordinates(15.0, 37.0)),
            width: 400.0,
            height: 300.0,
            unit: GeoUnit::Mi,
        });
        assert_eq!(
            words(rectangle),
            ["FROMLONLAT", "15", "37", "BYBOX", "400", "300", "mi"]
        );
        let polygon = GeosearchShape::Polygon(vec![
            coordinates(1.0, 2.0),
            coordinates(3.0, 4.0),
            coordinates(5.0, 6.0),
        ]);
        assert_eq!(
            words(polygon),
            ["BYPOLYGON", "3", "1", "2", "3", "4", "5", "6"]
        );
        assert_eq!(
            words_error(GeosearchShape::Polygon(vec![])),
            "vertices requires at least one item"
        );
    }

    #[test]
    fn geosearch_count() {
        assert_eq!(
            words(GeosearchCount {
                count: 5,
                any: true
            }),
            ["COUNT", "5", "ANY"]
        );
        assert_eq!(
            words(GeosearchCount {
                count: 5,
                any: false
            }),
            ["COUNT", "5"]
        );
    }

    #[test]
    fn geosearch_with_result() {
        assert_eq!(
            from::<GeosearchWithResult>(bulk("Palermo")),
            GeosearchWithResult {
                member: "Palermo".into(),
                distance: None,
                hash: None,
                coordinates: None,
            }
        );
        let value = array(vec![
            bulk("Palermo"),
            bulk("190.4424"),
            int(3479099956230698),
            bulks(&["13.5", "38.25"]),
        ]);
        assert_eq!(
            from::<GeosearchWithResult>(value),
            GeosearchWithResult {
                member: "Palermo".into(),
                distance: Some(190.4424),
                hash: Some(3479099956230698),
                coordinates: Some(coordinates(13.5, 38.25)),
            }
        );
        let value = array(vec![bulk("Palermo"), int(3479099956230698)]);
        assert_eq!(
            from::<GeosearchWithResult>(value),
            GeosearchWithResult {
                member: "Palermo".into(),
                distance: None,
                hash: Some(3479099956230698),
                coordinates: None,
            }
        );
        assert_eq!(
            from_error::<GeosearchWithResult>(array(vec![
                bulk("Palermo"),
                RespValue::Boolean(true)
            ])),
            "unexpected reply: boolean<true>"
        );
    }
}
