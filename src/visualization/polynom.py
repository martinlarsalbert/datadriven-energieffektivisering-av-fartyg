import sklearn
from sklearn.preprocessing import PolynomialFeatures
from sklearn.feature_selection import SelectKBest
import numpy as np
import sympy as sp

def find_polynomial_feature(model):
    found = False
    for part in model:
        if isinstance(part, PolynomialFeatures):
            polynomial_features = part
            found = True
            break
    if not found:
        raise ValueError('model pipeline must contain an instance of PolynomialFeatures')
    
    return polynomial_features

def find_select_k_best(model):
    found = False
    for part in model:
        if isinstance(part, SelectKBest):
            select_k_best = part
            found = True
            break
    if not found:
        raise ValueError('model pipeline must contain an instance of SelectKBest')
        
    return select_k_best

def model_to_string(model:sklearn.pipeline.Pipeline, feature_names:list, divide=' '):
    
    # Find polynomial features:
    polynomial_features = find_polynomial_feature(model=model)
    
    # Find select_k_best:
    select_k_best = find_select_k_best(model=model)
    
    polynomial_feature_names = np.array(polynomial_features.get_feature_names())
    best_polynomial_feature_names = polynomial_feature_names[select_k_best.get_support()]
    
    predictor = model[-1]  # Last item in the pipeline is assumed to be the precictor
    coefficients = predictor.coef_
    interception = predictor.intercept_
    
    x_names = ['x%i'%i for i in range(len(feature_names))]
    
    expression = ''
    expression+='%f' % interception
    for part,coefficient in zip(best_polynomial_feature_names,coefficients):
        
        nice_part = part.replace(' ','*')
        super_nice_part = nice_part
        for feature_name,x in zip(feature_names,x_names):
            super_nice_part=super_nice_part.replace(x,feature_name)
        
        if coefficient==0:
            continue
        elif coefficient<0:
            sign=''
        else:
            sign='+'
        
        sub_part = '%s%s%s%f*%s' % (divide,sign,divide,coefficient,super_nice_part)
    
        
        expression+=sub_part
    
    return expression
    
from sympy.parsing.sympy_parser import parse_expr
def model_to_sympy(model:sklearn.pipeline.Pipeline, feature_names:list, label='y'):
    
    model_string = model_to_string(model=model, feature_names=feature_names)
    model_string = model_string.replace('^','**')
    lhs = sp.Symbol(label)
    rhs = parse_expr(model_string)
    sympy_expression = sp.Eq(lhs=lhs, rhs=rhs)
    return sympy_expression