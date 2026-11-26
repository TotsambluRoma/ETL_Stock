from copy import deepcopy
from io import BytesIO

import numpy as np
import pandas as pd
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

from .config import FEATURES
from .market import schedule


def training_examples(prices: pd.DataFrame, features: pd.DataFrame, through) -> pd.DataFrame:
    if prices.empty or features.empty:
        raise ValueError("Сначала загрузите котировки и признаки.")
    sessions = schedule(prices["trade_date"].min().date(), through).index
    close = prices.set_index("trade_date")["close"].reindex(sessions)
    labels = pd.DataFrame({
        "target_date": sessions.to_series().shift(-1),
        "target_close": close.shift(-1),
    }, index=sessions)
    examples = features.join(labels, on="trade_date")
    return examples.dropna(subset=["target_date", "target_close"]).sort_values("target_date")


def network():
    return nn.Sequential(
        nn.Linear(len(FEATURES), 24), nn.ReLU(),
        nn.Linear(24, 12), nn.ReLU(),
        nn.Linear(12, 6), nn.ReLU(),
        nn.Linear(6, 1),
    )


def fit(examples: pd.DataFrame, through) -> tuple[bytes, dict]:
    n = len(examples)
    if n < 40:
        raise ValueError("Для обучения нужно хотя бы 40 полных пар признаков и цены.")
    first, second = int(n * 0.70), int(n * 0.85)
    train, validation, test = examples.iloc[:first], examples.iloc[first:second], examples.iloc[second:]
    columns = list(FEATURES)
    mean = train[columns].mean()
    scale = train[columns].std(ddof=0).replace(0, 1)

    def tensors(part):
        normalized = (part[columns] - mean) / scale
        return (
            torch.tensor(normalized.to_numpy(), dtype=torch.float32),
            torch.tensor(part[["close"]].to_numpy(), dtype=torch.float32),
            torch.tensor(part[["target_close"]].to_numpy(), dtype=torch.float32),
        )

    torch.set_num_threads(1)
    torch.manual_seed(42)
    model = network()
    loss_fn = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    batches = DataLoader(TensorDataset(*tensors(train)), batch_size=64, shuffle=True,
                         generator=torch.Generator().manual_seed(42))
    val_x, val_close, val_y = tensors(validation)
    best_loss, best_epoch, best_state = float("inf"), 0, None
    for epoch in range(1, 81):
        model.train()
        for x, close, actual in batches:
            optimizer.zero_grad()
            loss = loss_fn(close + model(x), actual)
            loss.backward()
            optimizer.step()
        model.eval()
        with torch.inference_mode():
            val_mse = loss_fn(val_close + model(val_x), val_y).item()
        if val_mse < best_loss:
            best_loss, best_epoch = val_mse, epoch
            best_state = deepcopy(model.state_dict())
        elif epoch - best_epoch >= 10:
            break
    if best_state is None:
        raise ValueError("Обучение не дало конечной ошибки на валидации.")
    model.load_state_dict(best_state)

    test_x, last_close, actual = tensors(test)
    with torch.inference_mode():
        test_price = (last_close + model(test_x)).numpy().ravel().astype(float)
    if not np.isfinite(test_price).all() or (test_price <= 0).any():
        raise ValueError("Сеть выдала некорректную цену на тестовых данных.")
    error = test_price - actual.numpy().ravel()
    mse = float(np.mean(error ** 2))
    metrics = {
        "mse": mse,
        "mae": float(np.mean(np.abs(error))),
        "rmse": mse ** 0.5,
        "mse_baseline": float(np.mean((last_close.numpy().ravel() - actual.numpy().ravel()) ** 2)),
        "train_count": len(train),
        "validation_count": len(validation),
        "test_count": len(test),
        "best_epoch": best_epoch,
        "test_from": test["target_date"].iloc[0].date().isoformat(),
        "test_through": test["target_date"].iloc[-1].date().isoformat(),
    }
    buffer = BytesIO()
    torch.save({
        "weights": best_state,
        "features": columns,
        "mean": mean.to_list(),
        "scale": scale.to_list(),
        "trained_through": through.isoformat(),
        "metrics": metrics,
    }, buffer)
    return buffer.getvalue(), metrics


def predict(weights: bytes, inputs: dict, as_of) -> float | None:
    parameters = torch.load(BytesIO(weights), map_location="cpu", weights_only=True)
    if parameters["features"] != list(FEATURES):
        raise ValueError("Порядок признаков модели не совпадает с рабочей таблицей.")
    if parameters["trained_through"] > as_of.isoformat():
        return None
    x = np.array([inputs[field] for field in FEATURES], dtype=float)
    standardized = (x - parameters["mean"]) / parameters["scale"]
    model = network()
    model.load_state_dict(parameters["weights"])
    model.eval()
    with torch.inference_mode():
        price = inputs["close"] + model(torch.tensor([standardized.tolist()], dtype=torch.float32)).item()
    if not np.isfinite(price) or price <= 0:
        raise ValueError("Модель вернула недопустимую цену.")
    return price
