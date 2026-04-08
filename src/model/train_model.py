"""Train a Random Forest model to predict dispute_count."""

import joblib
import logging
import os
from pathlib import Path

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from model.build_features import DB_PATH, encode_features, load_joined_data

log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
MODEL_PATH = DATA_DIR / "model.joblib"

TARGET = "dispute_count"
RANDOM_STATE = 42


def main() -> None:
    if not DB_PATH.exists():
        log.error("Database not found: %s", DB_PATH)
        return

    df = load_joined_data()
    df = encode_features(df)

    y = df[TARGET]
    X = df.drop(columns=[TARGET])

    log.info("Features: %d, Samples: %d", X.shape[1], X.shape[0])

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=RANDOM_STATE
    )

    log.info("Train: %d, Test: %d", len(X_train), len(X_test))

    model = RandomForestRegressor(n_estimators=100, random_state=RANDOM_STATE)
    model.fit(X_train, y_train)

    # Training metrics
    y_train_pred = model.predict(X_train)
    log.info("=== Training Results ===")
    log.info("  R2:   %.4f", r2_score(y_train, y_train_pred))
    log.info("  MAE:  %.4f", mean_absolute_error(y_train, y_train_pred))
    log.info("  RMSE: %.4f", root_mean_squared_error(y_train, y_train_pred))

    # Test metrics
    y_test_pred = model.predict(X_test)
    log.info("=== Test Results ===")
    log.info("  R2:   %.4f", r2_score(y_test, y_test_pred))
    log.info("  MAE:  %.4f", mean_absolute_error(y_test, y_test_pred))
    log.info("  RMSE: %.4f", root_mean_squared_error(y_test, y_test_pred))

    # Feature importance (top 10)
    importances = sorted(
        zip(X.columns, model.feature_importances_),
        key=lambda x: x[1],
        reverse=True,
    )
    log.info("=== Top 10 Features ===")
    for name, imp in importances[:10]:
        log.info("  %-30s %.4f", name, imp)

    joblib.dump(model, MODEL_PATH)
    log.info("Model saved to: %s", MODEL_PATH)


if __name__ == "__main__":
    main()
