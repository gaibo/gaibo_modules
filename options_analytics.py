import numpy as np
from scipy.stats import norm
from scipy.optimize import root


def black_76(is_call, t, k, f, r, sigma):
    """ Price options using Black-76 model (options on futures, bond options, swaptions, etc.)
    :param is_call: Boolean for whether it is a call option
    :param t: time to expiry in years
    :param k: strike
    :param f: forward price
    :param r: risk-free rate
    :param sigma: implied volatility
    :return: option premium as a number or array of numbers, depending on input format
    """
    d1 = (np.log(f/k) + (sigma**2/2)*t) / (sigma*np.sqrt(t))
    d2 = d1 - sigma*np.sqrt(t)
    if isinstance(is_call, bool):
        # Single number form
        if is_call:
            return np.exp(-r * t) * (f * norm.cdf(d1) - k * norm.cdf(d2))
        else:
            return np.exp(-r * t) * (k * norm.cdf(-d2) - f * norm.cdf(-d1))
    else:
        # Array form
        arr_out = np.zeros_like(sigma)
        call_arr = np.exp(-r * t) * (f * norm.cdf(d1) - k * norm.cdf(d2))
        put_arr = np.exp(-r * t) * (k * norm.cdf(-d2) - f * norm.cdf(-d1))
        arr_out[is_call] = call_arr[is_call]
        arr_out[~is_call] = put_arr[~is_call]
        return arr_out


def implied_vol_b76(is_call, t, k, f, r, prem):
    """ Back out implied volatility of options using Black-76 model (options on futures, bond options, swaptions, etc.)
    :param is_call: Boolean for whether it is a call option
    :param t: time to expiry in years
    :param k: strike
    :param f: forward price
    :param r: risk-free rate
    :param prem: option price
    :return: option implied volatility as a number or array of numbers, depending on input format
    """
    solved_root = \
        root(lambda sigma: black_76(is_call, t, k, f, r, sigma) - prem,
             x0=np.ones_like(prem), tol=None)
    if isinstance(is_call, bool):
        # Single number form
        return solved_root.x[0]
    else:
        # Array form
        return solved_root.x


def vega_b76(t, k, f, r, sigma):
    """ Calculate vega of options using Black-76 model (options on futures, bond options, swaptions, etc.)
    :param t: time to expiry in years
    :param k: strike
    :param f: forward price
    :param r: risk-free rate
    :param sigma: implied volatility
    :return: vega as a number or array of numbers, depending on input format
    """
    d1 = (np.log(f/k) + (sigma**2/2)*t) / (sigma*np.sqrt(t))
    return np.exp(-r*t)*f * norm.pdf(d1)*np.sqrt(t) * 0.01
