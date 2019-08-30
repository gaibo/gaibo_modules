import numpy as np
from scipy.stats import norm
from scipy.optimize import root


# Black-76 for premium for options on futures
# black_76(call Boolean, time to expiry, strike, forward price, risk-free rate, implied volatility)
def black_76(is_call, t, k, f, r, sigma):
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


# Solve root of Black-76 with vol guess minus actual prem for implied volatility for options on futures
# implied_vol_b76(call Boolean, time to expiry, strike, forward price, risk-free rate, option price)
def implied_vol_b76(is_call, t, k, f, r, prem):
    solved_root = \
        root(lambda sigma: black_76(is_call, t, k, f, r, sigma) - prem,
             x0=np.ones_like(prem), tol=None)
    if isinstance(is_call, bool):
        # Single number form
        return solved_root.x[0]
    else:
        # Array form
        return solved_root.x


# Vega (Black-76)
# vega_b76(time to expiry, strike, forward price, risk-free rate, implied volatility)
def vega_b76(t, k, f, r, sigma):
    d1 = (np.log(f/k) + (sigma**2/2)*t) / (sigma*np.sqrt(t))
    return np.exp(-r*t)*f * norm.pdf(d1)*np.sqrt(t) * 0.01
