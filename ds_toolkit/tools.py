from matplotlib import pyplot as plt
import matplotlib.ticker as plticker
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np


def rebin(series_, thresh, side='underflow',percentile=False):
    '''
    Function that fills underflow/overflow bins for visualization
    
    args:
        * series_ : data structure to be rebinned : type = pandas series 
        * thresh : threshold : type = int or float
        * side : which bin (underflow or overflow) to put values : type = string
        * percentile: determines whether thresh is interpreted as percentile or in units of series : type bool
    returns:
        * : type = pandas series
    '''
    #correct a bug in python where chained indexing warning gets confused so turn it off
    pd.options.mode.chained_assignment = None  # default='warn'
    
    #make a local copy
    series = series_.copy(deep=True)
    
    #redefine threshold based on percentile
    if percentile:
        thresh = np.nanpercentile(series,thresh)
        #z-score filtering could be useful later
        #z_score = (thresh - series.mean()) / series.std(ddof=0)
    
    if side is 'underflow':
        series.loc[series <= thresh] = thresh
    elif side is 'overflow':
        series.loc[series >= thresh] = thresh
    else:
        print('need to specify underflow or overflow')
    return series



def hist_labeler(series_in,label_in=None):
    '''
    Formats histograms labels
    inputs: pd series
    returns: label string
    '''
    null_cnt = series_in.isna().sum()
    entries = series_in.shape[0]
    label = 'Entries {}\n$\mu$={:.4f}'.format(entries,series_in.mean())
    if null_cnt	> 1:
        label = '{} entries, {} ({:.1f}%) null\n$\mu$={:.4f}'.format(entries,null_cnt,null_cnt*100./entries,series_in.mean())
        
    if label_in:
        label = '{} {}'.format(label_in,label)
    return label

def single_hist(df_in_,col,title=None,xlabel=None,ylabel=None,xrng=None,**kwargs):
    '''
    Plot Single histogram
    Usage example: single_hist(df,'support_lag1_ratio',bins=60,kde=False,xrng=(0.75,1.5))
    '''
    #make a local copy to avoid altering the original
    df_in = df_in_.copy(deep=True)

    #treatment of infs
    df_in[col] = df_in[col].replace([np.inf, -np.inf], np.nan)
    
    if xrng:
        df_in[col] = rebin(df_in[col],xrng[0],'underflow')
        df_in[col] = rebin(df_in[col],xrng[1],'overflow')
    else:
        df_in[col] = rebin(df_in[col],99,'overflow',percentile=True)
        df_in[col] = rebin(df_in[col],1,'underflow',percentile=True)

    hist_label = hist_labeler(df_in[col])
    
    plt.rc('font', family='serif')
    f,ax = plt.subplots(figsize=(10,5))
    ax.grid()

    sns.distplot(df_in[col],label=hist_label,**kwargs)
    plt.title(title)
    plt.legend(loc='best',fontsize=10)
    if xlabel:
        ax.set_xlabel(xlabel)



        
def plot_2dists(df1_,df2_,col1,col2,label1=None, label2=None,xrng=None,**kwargs):
    '''
    inputs: df1 type=pd
            df2 type=p2
            col1 type=string desc: column to be plotted in df1
            col2 type=string desc: column to be plotted in df2
            label1 type=string desc: label for df1
            label2 type=string desc: label for df2
            n_bins type=int desc: number of bins in output hist
    outputs: none, produces normalized histogram of var_in of df1 and df2 on same axis

    '''

    #change local copies only
    df1 = df1_.copy(deep=True)
    df2 = df2_.copy(deep=True)
    
    #treatment of infs
    df1[col1] = df1[col1].replace([np.inf, -np.inf], np.nan)
    df2[col2] = df2[col2].replace([np.inf, -np.inf], np.nan)
    
    plt.rc('font', family='serif')
    f,ax = plt.subplots(figsize=(10,5))
    #ax.grid()

    if xrng is None:
        df1[col1] = rebin(df1[col1],99,'overflow',percentile=True)
        df1[col1] = rebin(df1[col1],1,'underflow',percentile=True)
        df2[col2] = rebin(df2[col2],99,'overflow',percentile=True)
        df2[col2] = rebin(df2[col2],1,'underflow',percentile=True)        
        
        #set axis limits
        xmin = df2[col2].min()
        xmax = df1[col1].max()
        if df1[col1].min() < df2[col2].min():
            xmin = df1[col1].min()
            
        if df1[col1].max() < df2[col2].max():
            xmax = df2[col2].max()

        xrnge = (xmin, xmax*1.01)
    else:
        xrnge = xrng

    #plotting
    if not label1:
        label1=col1
    if not label2:
        label2=col2

    hist_label1 = hist_labeler(df1[col1],label1)
    hist_label2 = hist_labeler(df2[col2],label2)

    #stop here
    df1[col1] = rebin(df1[col1],xrnge[0],'underflow')
    df1[col1] = rebin(df1[col1],xrnge[1],'overflow')
    df2[col2] = rebin(df2[col2],xrnge[0],'underflow')
    df2[col2] = rebin(df2[col2],xrnge[1],'overflow')    
    
    sns.distplot(df1[col1],label=hist_label1,hist_kws={"range":(xrnge[0],xrnge[1])},**kwargs)
    sns.distplot(df2[col2],label=hist_label2,hist_kws={"range":(xrnge[0],xrnge[1])},**kwargs)

    plt.grid()
    plt.legend(loc='best',fontsize=10)
    ax.set_xlim(xrnge[0],xrnge[1])
    ax.set(xlabel=col1, ylabel='counts')
    plt.show()


def correlation_plot(X_in,features,title_txt = ''):
    scaler = MinMaxScaler()
    X_normd = scaler.fit_transform(X_in)
    
    features_corr = pd.DataFrame(X_normd,columns=features).corr()
    features_corr = features_corr.round(2)

    fig, ax = plt.subplots(figsize=(14, 12))
    sns.heatmap(features_corr, 
                xticklabels=features_corr.columns,
                yticklabels=features_corr.columns, 
                annot=True,
                ax=ax)
    ax.set_title('Features Correlation {}'.format(title_txt), size=15)
    fig.show()

def plt_timeseries(var,timevar,df,title):
    #f,ax = plt.subplots(2,1,sharex=True,figsize=(15,7))
    f,ax = plt.subplots(figsize=(15,7))

    ax.scatter(df[timevar],df[var],color='green')
    # format the ticks
    f.autofmt_xdate()

    #Spacing between each line
    intervals = float(1)

    loc = plticker.MultipleLocator(base=intervals)
    ax.xaxis.set_major_locator(loc)
    #ax[0].yaxis.set_major_locator(loc)

    # Add the grid
    plt.grid()
    plt.xlabel(timevar)
    ax.set_ylabel(var)
    ax.grid(which='major', axis='both', linestyle='-')
    plt.xticks(fontsize=10)
    plt.title(title)
    plt.show()


################
##
# Plots categorical values directly from pandas
# inputs: str_list: list of pandas columns to plot
#         df_in: pandas dataframe
# output: None
##
################
def plot_strings(str_list,df_in):

    for plot in str_list:
        try:
            df_in[plot].hist(xrot=90)
            plt.ylabel('counts')
            plt.title(plot)
            plt.show()
        except Exception as e:
            print('ERROR plotting {}, exception={}'.format(plot,e))


def dedupe(df_in):
    '''
    De-duplicate Pandas datafarme and print info
    input: pandas dataframe
    returns: de-duped pandas dataframe  
    '''
    print('shape before de-dupe:{}'.format(df_in.shape))
    df_in_dedupe = df_in.drop_duplicates()
    print('shape after de-dupe:{}'.format(df_in_dedupe.shape))
    print('{} duplicate rows found'.format(df_in.shape[0]-df_in_dedupe.shape[0]))
    return df_in_dedupe


def check_quality(df_in_):
    '''
    Data Quality checker for Pandas, prints duplicates, null content of cols
    inputs: df_in_: pandas dataframe
    returns: None
    '''
    df_in = dedupe(df_in_)
    colz = df_in.columns

    nulls_counts = []
    for col in colz:
        pct_null = df_in[col].isna().sum()*100./df_in.shape[0]
        if pct_null > 0:
            nulls_counts.append(('{} is {:.1f}% NULL'.format(col, pct_null),pct_null))

    sorted_counts = sorted(nulls_counts,key=lambda x:x[1],reverse=True)
    print('\n---- \n')
    print('columns with null rows:')
    for j in sorted_counts:
        print(j[0])
    
