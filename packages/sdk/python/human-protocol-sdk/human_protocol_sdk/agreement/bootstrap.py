import numpy as np
from typing import Sequence, Callable, Optional
from warnings import warn

from human_protocol_sdk.agreement.utils import NormalDistribution


def bootstrap_ci(
    data: Sequence,
    statistic_fn: Callable,
    n_iterations: int = 1000,
    n_sample: Optional[int] = None,
    ci=0.95,
    algorithm="bca",
) -> tuple:
    """Returns the confidence interval for the boostrap estimate of the given
    statistic.

    Args:
        data: Data to estimate the statistic.
        statistic_fn: Function to calculate the statistic. `f(data)` must
            return the statistic.
        n_iterations: Number of bootstrap samples to use for the estimate.
        n_sample: If provided, determines the size of each bootstrap sample
            drawn from the data. If omitted, is equal to the length of the
            data.
        ci: Size of the confidence interval.
        algorithm: Which algorithm to use for the confidence interval
            estimation. "bca" uses the "Bias Corrected Bootstrap with
            Acceleration", "percentile" simply takes the appropriate
            percentiles from the bootstrap distribution.
    """
    data = np.asarray(data)

    if n_iterations < 1:
        raise ValueError(
            f"n_iterations must be a positive integer, but were {n_iterations}"
        )

    n_data = len(data)
    if n_data < 30:
        warn(
            "Dataset size is low, bootstrap estimate might be inaccurate. For accurate results, make sure to provide at least 30 data points."
        )

    if n_sample is None:
        n_sample = n_data
    elif n_sample < 1:
        raise ValueError(f"n_sample must be a positive integer, but was {n_sample}")

    if not (0.0 <= ci <= 1.0):
        raise ValueError(f"ci must be a float within [0.0, 1.0], but was {ci}")

    # bootstrap estimates
    theta_b = np.empty(n_iterations, dtype=float)
    for i in range(n_iterations):
        idx = np.random.randint(n_data - 1, size=(n_sample,))
        sample = data[idx]
        theta_b[i] = statistic_fn(sample)

    match algorithm:
        case "percentile":
            alpha = 1.0 - ci
            alpha /= 2.0
            q = np.asarray([alpha, 1.0 - alpha])
        case "bca":
            # acceleration: estimate a from jackknife bootstrap
            theta_hat = statistic_fn(data)
            jn_idxs = ~np.eye(n_data, dtype=bool)
            theta_jn = np.empty(n_data, dtype=float)
            for i in range(n_data):
                theta_jn[i] = (n_data - 1) * (
                    theta_hat - statistic_fn(data[jn_idxs[i]])
                )

            a = (np.sum(theta_jn**3) / np.sum(theta_jn**2, axis=-1) ** 1.5) / 6

            alpha = 1.0 - ci
            alpha /= 2
            q = np.asarray([alpha, 1.0 - alpha])

            # bias correction
            N = NormalDistribution()
            ppf = np.vectorize(N.ppf)
            cdf = np.vectorize(N.cdf)

            # bias term. discrepancy between bootrap values and estimated value
            z_0 = ppf(np.mean(theta_b < theta_hat))
            z_u = ppf(q)
            z_diff = z_0 + z_u

            q = cdf(z_0 + (z_diff / (1 - a * z_diff)))
        case _:
            raise ValueError(f"Algorithm '{algorithm}' is not available!")

    # sanity checks
    if np.any(np.isnan(q)):
        warn(
            f"q contains NaN values. Input data is probably invalid. Interval will be (nan, nan). data: {data}"
        )
        ci_low = ci_high = np.nan
    else:
        if np.any((q < 0.0) | (q > 1.0)):
            warn(
                f"q ({q}) out of bounds. Input data is probably invalid. q will be clipped into interval [0.0, 1.0]. data: {data}"
            )
            q = np.clip(q, 0.0, 1.0)
        ci_low, ci_high = np.percentile(theta_b, q * 100)

    return (ci_low, ci_high), theta_b