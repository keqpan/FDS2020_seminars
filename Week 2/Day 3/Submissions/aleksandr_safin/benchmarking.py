from sklearn.model_selection import GridSearchCV
import joblib
import time
import numpy as np

class ModelFitter():
    def __init__(self, model, static_array=False):
        """
            static_array (bool): whether to convert dask arrays to numpy, while using with sklearn model
        """
        self.model = model
        self.static_array = static_array
        
    def fit_cv(self, X, y, n_folds, param_grid):
        if self.static_array:
            X_data = X.compute()
            y_data = y.compute().ravel()
        else:
            X_data = X
            y_data = y
            
        grid_search = GridSearchCV(self.model, param_grid, cv=n_folds, n_jobs=1)
        start_time = time.time()
        with joblib.parallel_backend("dask", scatter=[X_data, y_data]):
            grid_search.fit(X_data, y_data)
        elapsed_time = time.time() - start_time
        
        return elapsed_time, elapsed_time, grid_search.best_score_

def benchmark_models(X, y, models, n_folds, param_grid):
    """
        Measure time for grid search for each model
    """
    
    results = []
    for m in models:
        results.append(m.fit_cv(X, y, n_folds, param_grid))
    return np.asarray(results)